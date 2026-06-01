package com.docintel.audit_service.service;

import com.docintel.audit_service.dto.QueryExecutedEvent;
import com.docintel.audit_service.model.QueryAuditEntity;
import com.docintel.audit_service.repository.QueryAuditRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
public class QueryAuditConsumer {

    private final ObjectMapper objectMapper;
    private final QueryAuditRepository queryAuditRepository;

    @KafkaListener(
            topics = "${app.kafka.topic.query-executed}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consume(String message){
        try{
            QueryExecutedEvent event = objectMapper.readValue(message, QueryExecutedEvent.class);
            QueryAuditEntity audit = new QueryAuditEntity();
            audit.setUserId(event.getUserId());
            audit.setQuestion(event.getQuestion());
            audit.setAnswer(event.getAnswer());
            audit.setTimestamp(LocalDateTime.now());
            audit.setDocumentsQueried("embeddings_" + event.getUserId());

            queryAuditRepository.save(audit);
        }catch(Exception e){
            System.out.println("Failed to parse wuery: "+ e.getMessage());
        }
    }
}
