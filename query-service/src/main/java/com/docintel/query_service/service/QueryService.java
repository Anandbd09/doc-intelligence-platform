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

    public QueryResponse answerQuestion(String userId, String question){
        // Step 1 - convert question to embedding
        Response<Embedding> response = embeddingModel.embed(TextSegment.from(question));
        Embedding questionEmbedding = response.content();

        // Step 2 - search ChromaDB for similar chunks
        EmbeddingStore<TextSegment> store = ChromaEmbeddingStore.builder()
                .baseUrl(chromaUrl)
                .collectionName("embeddings_"+userId)
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

        String prompt = "Answer the question based only on the context below.\n\n" +
                "Context:\n" + context + "\n\n" +
                "Question: " + question;

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
