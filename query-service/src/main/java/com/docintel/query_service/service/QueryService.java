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
                .maxResults(5)
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

}
