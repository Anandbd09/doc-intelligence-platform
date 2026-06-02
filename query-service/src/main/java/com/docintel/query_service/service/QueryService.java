package com.docintel.query_service.service;

import com.docintel.query_service.dto.response.CrossSearchResponse;
import com.docintel.query_service.dto.response.QueryResponse;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.chat.ChatLanguageModel;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.output.Response;
import dev.langchain4j.store.embedding.EmbeddingMatch;
import dev.langchain4j.store.embedding.EmbeddingSearchRequest;
import dev.langchain4j.store.embedding.EmbeddingSearchResult;
import dev.langchain4j.store.embedding.EmbeddingStore;
import dev.langchain4j.store.embedding.chroma.ChromaEmbeddingStore;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class QueryService {
    private final EmbeddingModel embeddingModel;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ChatLanguageModel chatLanguageModel;
    private final RedisTemplate<String,String> redisTemplate;
    private final MongoTemplate mongoTemplate;

    @Value("${chromadb.url}")
    private String chromaUrl;

    @Value("${app.kafka.topic.query-executed}")
    private String queryExecutedTopic;

//    @Cacheable(value = "queryCache", key = "#docId + '_' + #question")
    public QueryResponse answerQuestion(String userId, String docId, String question) {
        // Step 1 - get conversation history from Redis
        String historyKey = "chat_" + userId + "_" + docId;
        List<String> history = redisTemplate.opsForList().range(historyKey, 0, -1);

        // Step 2 - convert question to embedding
        // Expand question for better retrieval
        String expandedQuery = question + " " + question + " definition explanation overview";
        Response<Embedding> response = embeddingModel.embed(TextSegment.from(expandedQuery));
        Embedding questionEmbedding = response.content();

        // Step 3 - search ChromaDB
        EmbeddingStore<TextSegment> store = ChromaEmbeddingStore.builder()
                .baseUrl(chromaUrl)
                .collectionName("embeddings_" + userId + "_" + docId)
                .build();
        EmbeddingSearchRequest searchRequest = EmbeddingSearchRequest.builder()
                .queryEmbedding(questionEmbedding)
                .maxResults(15)
                .build();
        EmbeddingSearchResult<TextSegment> searchResult = store.search(searchRequest);
        List<EmbeddingMatch<TextSegment>> matches = searchResult.matches();

        // Step 4 - build context
        String context = matches.stream()
                .map(match -> match.embedded().text())
                .collect(Collectors.joining("\n\n"));

        // Step 5 - build conversation history string
        String conversationHistory = "";
        if (history != null && !history.isEmpty()) {
            conversationHistory = "Previous conversation:\n" +
                    String.join("\n", history) + "\n\n";
        }

        // Step 6 - build prompt with history
        String prompt = """
        You are an intelligent document assistant with two modes:
        
        MODE 1 - Document Answer (preferred):
        If the answer exists in the provided context, answer from the document.
        Always cite that the answer comes from the document.
        
        MODE 2 - General Knowledge:
        If the answer is NOT in the provided context, answer from your general knowledge.
        BUT always start with: "This information is not in your document, but here's what I know: "
        
        Rules:
        - Always try document context first
        - Be clear, concise and well-structured
        - Use bullet points when listing items
        - For greetings like "hi/hello", respond friendly: "Hello! Ask me anything about your document or any general question!"
        - Detect the language of the CURRENT user question and answer ONLY in that language
        - If current question is in English → answer in English
        - If current question is in Hindi → answer in Hindi
        - If current question is in Kannada → answer in Kannada
        - Preserve technical terms as-is
        - Use conversation history ONLY for context of follow-up questions, NOT for language detection
        - Do not make up information when answering from document context
        
        %sContext from document:
        %s
        
        User Question: %s
        
        Answer:
        """.formatted(conversationHistory, context, question);

        // Step 7 - call LLM
        String answer = chatLanguageModel.generate(prompt);

        // Step 8 - save to conversation history in Redis (keep last 10)
        redisTemplate.opsForList().rightPush(historyKey, "User: " + question);
        redisTemplate.opsForList().rightPush(historyKey, "Assistant: " + answer);
        redisTemplate.opsForList().trim(historyKey, -10, -1);
        redisTemplate.expire(historyKey, 1, java.util.concurrent.TimeUnit.HOURS);

        // Step 9 - publish audit event
        String auditMessage = String.format(
                "{\"userId\":\"%s\",\"question\":\"%s\",\"answer\":\"%s\"}",
                userId, question, answer.replace("\"", "'"));
        kafkaTemplate.send(queryExecutedTopic, userId, auditMessage);

        // Step 10 - return response
        List<String> sources = new ArrayList<>();
        if (!answer.startsWith("This information is not in your document")) {
            sources = matches.stream()
                    .map(match -> match.embedded().text().substring(0, Math.min(50, match.embedded().text().length())))
                    .collect(Collectors.toList());
        }

        return new QueryResponse(answer, sources);
    }

    public QueryResponse summarizeDocument(String userId, String docId) {
        EmbeddingStore<TextSegment> store = ChromaEmbeddingStore.builder()
                .baseUrl(chromaUrl)
                .collectionName("embeddings_" + userId + "_" + docId)
                .build();

        // Use multiple searches to get diverse chunks from different parts
        String[] searchQueries = {
                "introduction overview what is this about",
                "main content key points important",
                "conclusion summary results findings"
        };

        List<String> allChunks = new ArrayList<>();

        for (String query : searchQueries) {
            Response<Embedding> response = embeddingModel.embed(TextSegment.from(query));
            EmbeddingSearchRequest searchRequest = EmbeddingSearchRequest.builder()
                    .queryEmbedding(response.content())
                    .maxResults(5)
                    .build();
            EmbeddingSearchResult<TextSegment> result = store.search(searchRequest);
            result.matches().forEach(match -> {
                String text = match.embedded().text();
                if (!allChunks.contains(text)) {
                    allChunks.add(text);
                }
            });
        }

        String context = String.join("\n\n", allChunks);

        String prompt = """
        You are an intelligent document assistant. Create a comprehensive summary of the document based on the provided content.
        
        Rules:
        - Start with a one-line overview of what the document is about
        - List the main topics covered using bullet points
        - Highlight key insights or important information
        - End with a brief conclusion
        - Keep the summary clear and concise
        - Do not make up information not present in the context
        
        Document content:
        %s
        
        Summary:
        """.formatted(context);

        String answer = chatLanguageModel.generate(prompt);

        String auditMessage = String.format(
                "{\"userId\":\"%s\",\"question\":\"SUMMARIZE\",\"answer\":\"%s\"}",
                userId, answer.replace("\"", "'"));
        kafkaTemplate.send(queryExecutedTopic, userId, auditMessage);

        List<String> sources = allChunks.stream()
                .map(chunk -> chunk.substring(0, Math.min(50, chunk.length())))
                .collect(Collectors.toList());
        System.out.println("Searching collection: embeddings_" + userId + "_" + docId);
        System.out.println("Total chunks found: " + allChunks.size());

        return new QueryResponse(answer, sources);
    }

    public CrossSearchResponse crossSearch(String userId, String question) {
        // Step 1 - get all documents for user from MongoDB
        org.springframework.data.mongodb.core.query.Query mongoQuery =
                new org.springframework.data.mongodb.core.query.Query(
                        org.springframework.data.mongodb.core.query.Criteria.where("userId").is(userId)
                                .and("status").is("READY")
                );

        List<Map> documents = mongoTemplate.find(mongoQuery, Map.class, "documents");
        System.out.println("Found " + documents.size() + " documents for user: " + userId);

        if (documents.isEmpty()) {
            return new CrossSearchResponse("No documents found for this user.", new ArrayList<>());
        }

        // Step 2 - generate question embedding
        Response<Embedding> embResponse = embeddingModel.embed(TextSegment.from(question));
        Embedding questionEmbedding = embResponse.content();

        // Step 3 - search all collections in parallel
        List<CompletableFuture<CrossSearchResponse.DocumentMatch>> futures = documents.stream()
                .map(doc -> CompletableFuture.supplyAsync(() -> {
                    String docId = doc.get("_id").toString();
                    String docName = (String) doc.getOrDefault("name", "Unknown");

                    try {
                        EmbeddingStore<TextSegment> store = ChromaEmbeddingStore.builder()
                                .baseUrl(chromaUrl)
                                .collectionName("embeddings_" + userId + "_" + docId)
                                .build();

                        EmbeddingSearchRequest searchRequest = EmbeddingSearchRequest.builder()
                                .queryEmbedding(questionEmbedding)
                                .maxResults(3)
                                .build();

                        EmbeddingSearchResult<TextSegment> result = store.search(searchRequest);

                        List<String> chunks = result.matches().stream()
                                .map(match -> match.embedded().text())
                                .collect(Collectors.toList());

                        if (chunks.isEmpty()) return null;

                        return new CrossSearchResponse.DocumentMatch(docId, docName, chunks);
                    } catch (Exception e) {
                        System.err.println("Failed to search doc " + docId + ": " + e.getMessage());
                        return null;
                    }
                }))
                .collect(Collectors.toList());

        // Step 4 - collect results
        List<CrossSearchResponse.DocumentMatch> matches = futures.stream()
                .map(CompletableFuture::join)
                .filter(match -> match != null && !match.getRelevantChunks().isEmpty())
                .collect(Collectors.toList());

        System.out.println("Found relevant content in " + matches.size() + " documents");

        if (matches.isEmpty()) {
            return new CrossSearchResponse(
                    "I could not find relevant information across your documents.",
                    new ArrayList<>()
            );
        }

        // Step 5 - build context from all documents
        StringBuilder contextBuilder = new StringBuilder();
        for (CrossSearchResponse.DocumentMatch match : matches) {
            contextBuilder.append("From document '").append(match.getDocName()).append("':\n");
            match.getRelevantChunks().forEach(chunk ->
                    contextBuilder.append(chunk).append("\n\n")
            );
        }

        // Step 6 - call LLM
        String prompt = """
        You are an intelligent document assistant. Answer the user's question based on content from multiple documents.
        
        Rules:
        - Answer based ONLY on the provided context
        - Mention which document each piece of information comes from
        - Be clear and well-structured
        - If information is not found, say so clearly
        
        Context from multiple documents:
        %s
        
        User Question: %s
        
        Answer:
        """.formatted(contextBuilder.toString(), question);

        String answer = chatLanguageModel.generate(prompt);

        return new CrossSearchResponse(answer, matches);
    }

    public QueryResponse compareDocuments(String userId, String docId1, String docId2, String question) {
        // Step 1 - generate question embedding
        Response<Embedding> embResponse = embeddingModel.embed(TextSegment.from(question));
        Embedding questionEmbedding = embResponse.content();

        // Step 2 - search both documents in parallel
        CompletableFuture<String> doc1Future = CompletableFuture.supplyAsync(() -> {
            EmbeddingStore<TextSegment> store = ChromaEmbeddingStore.builder()
                    .baseUrl(chromaUrl)
                    .collectionName("embeddings_" + userId + "_" + docId1)
                    .build();
            EmbeddingSearchRequest searchRequest = EmbeddingSearchRequest.builder()
                    .queryEmbedding(questionEmbedding)
                    .maxResults(5)
                    .build();
            return store.search(searchRequest).matches().stream()
                    .map(match -> match.embedded().text())
                    .collect(Collectors.joining("\n\n"));
        });

        CompletableFuture<String> doc2Future = CompletableFuture.supplyAsync(() -> {
            EmbeddingStore<TextSegment> store = ChromaEmbeddingStore.builder()
                    .baseUrl(chromaUrl)
                    .collectionName("embeddings_" + userId + "_" + docId2)
                    .build();
            EmbeddingSearchRequest searchRequest = EmbeddingSearchRequest.builder()
                    .queryEmbedding(questionEmbedding)
                    .maxResults(5)
                    .build();
            return store.search(searchRequest).matches().stream()
                    .map(match -> match.embedded().text())
                    .collect(Collectors.joining("\n\n"));
        });

        String context1 = doc1Future.join();
        String context2 = doc2Future.join();

        // Step 3 - build comparison prompt
        String prompt = """
        You are a document comparison assistant.
        Compare the information from Document 1 and Document 2 based only on the provided context.
        
        Instructions:
        - Identify similarities
        - Identify differences
        - Highlight any conflicting information
        - If information is missing in either document, say so
        - Do not make assumptions outside the provided context
        
        Document 1:
        ----------------
        %s
        
        Document 2:
        ----------------
        %s
        
        User Question: %s
        
        Provide your answer in this format:
        Summary:
        ...
        Similarities:
        - ...
        Differences:
        - ...
        Conflicts (if any):
        - ...
        """.formatted(context1, context2, question);

        String answer = chatLanguageModel.generate(prompt);

        // Step 4 - audit
        String auditMessage = String.format(
                "{\"userId\":\"%s\",\"question\":\"COMPARE:%s\",\"answer\":\"%s\"}",
                userId, question, answer.replace("\"", "'"));
        kafkaTemplate.send(queryExecutedTopic, userId, auditMessage);

        return new QueryResponse(answer, new ArrayList<>());
    }
}
