package com.docintel.document_service.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

@Component
@RequiredArgsConstructor
public class DocDeletedConsumer {

    private final ObjectMapper objectMapper;

    @Value("${chromadb.url}")
    private String chromaUrl;

    @KafkaListener(topics = "${app.kafka.topic.doc-deleted}", groupId = "embedding-service-group")
    public void consumeDocDeleted(String message) {
        try {
            Map<?, ?> event = objectMapper.readValue(message, Map.class);
            String docId = (String) event.get("docId");
            String userId = (String) event.get("userId");

            String collectionName = "embeddings_" + userId + "_" + docId;

            RestTemplate restTemplate = new RestTemplate();
            restTemplate.delete(chromaUrl + "/api/v1/collections/" + collectionName);

            System.out.println("Deleted ChromaDB collection: " + collectionName);
        } catch (Exception e) {
            System.err.println("Failed to delete ChromaDB collection: " + e.getMessage());
        }
    }
}