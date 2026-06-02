package com.docintel.query_service.service;

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
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class QueryService {
    private final EmbeddingModel embeddingModel;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ChatLanguageModel chatLanguageModel;

    @Value("${chromadb.url}")
    private String chromaUrl;

    @Value("${app.kafka.topic.query-executed}")
    private String queryExecutedTopic;

    @Cacheable(value = "queryCache", key = "#docId + '_' + #question")
    public QueryResponse answerQuestion(String userId, String docId, String question){
        // Step 1 - convert question to embedding
        Response<Embedding> response = embeddingModel.embed(TextSegment.from(question));
        Embedding questionEmbedding = response.content();

        // Step 2 - search ChromaDB for similar chunks
        EmbeddingStore<TextSegment> store = ChromaEmbeddingStore.builder()
                .baseUrl(chromaUrl)
                .collectionName("embeddings_" + userId + "_" + docId)
                .build();
        EmbeddingSearchRequest searchRequest = EmbeddingSearchRequest.builder()
                .queryEmbedding(questionEmbedding)
                .maxResults(10)
                .build();

        EmbeddingSearchResult<TextSegment> searchResult = store.search(searchRequest);
        List<EmbeddingMatch<TextSegment>> matches = searchResult.matches();


        // Step 3 - build context from matches

        String context = matches.stream()
                .map(match -> match.embedded().text())
                .collect(Collectors.joining("\n\n"));
        // Step 4 - build prompt

        String prompt = """
    You are an intelligent document assistant. Answer the user's question based ONLY on the provided context.
    
    Rules:
    - Be clear, concise and well-structured
    - Use bullet points or numbered lists when listing multiple items
    - If the answer is not in the context, say "I could not find this information in the document"
    - Do not make up information or use outside knowledge
    - Keep the answer focused and relevant
    - If quoting directly, mention it comes from the document
    
    Context from document:
    %s
    
    User Question: %s
    
    Answer:
    """.formatted(context, question);

        // Step 5 - call LLM and return response

        String answer = chatLanguageModel.generate(prompt);

// publish audit event to Kafka
        String auditMessage = String.format(
                "{\"userId\":\"%s\",\"question\":\"%s\",\"answer\":\"%s\"}", userId, question,answer.replace("\"", "'"));
        kafkaTemplate.send(queryExecutedTopic, userId, auditMessage);

// build and return QueryResponse
        List<String> sources = matches.stream()
                .map(match -> match.embedded().text().substring(0, 50))
                .collect(Collectors.toList());

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

}
