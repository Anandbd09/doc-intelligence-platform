package com.docintel.embedding_service.service;

import com.docintel.embedding_service.dto.DocUploadedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class DocUploadedConsumer {

    private final ObjectMapper objectMapper;
    private final EmbeddingService embeddingService;
    @KafkaListener(
            topics = "${app.kafka.topic.doc-uploaded}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
   public void consume(String message){
        try{
            DocUploadedEvent event = objectMapper.readValue(message, DocUploadedEvent.class);
            embeddingService.processDocument(event);
        }
        catch(Exception e){
            System.out.println("Failed to parse message: " + e.getMessage());
        }
    }
}
