package com.docintel.query_service.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;

@Component
@RequiredArgsConstructor
public class DocDeletedConsumer {

    private final RedisTemplate<String, String> redisTemplate;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "${app.kafka.topic.doc-deleted}", groupId = "query-service-delete-group")
    public void consumeDocDeleted(String message) {
        try {
            Map<?, ?> event = objectMapper.readValue(message, Map.class);
            String docId = (String) event.get("docId");

            // Clear all Redis cache entries for this document
            Set<String> keys = redisTemplate.keys("queryCache::" + docId + "*");
            if (keys != null && !keys.isEmpty()) {
                redisTemplate.delete(keys);
                System.out.println("Cleared " + keys.size() + " Redis cache entries for doc: " + docId);
            }

            // Clear conversation history
            Set<String> historyKeys = redisTemplate.keys("chat_*_" + docId);
            if (historyKeys != null && !historyKeys.isEmpty()) {
                redisTemplate.delete(historyKeys);
                System.out.println("Cleared conversation history for doc: " + docId);
            }

        } catch (Exception e) {
            System.err.println("Failed to clear Redis for deleted doc: " + e.getMessage());
        }
    }
}