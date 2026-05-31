package com.docintel.embedding_service.service;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class DocUploadedConsumer {
    @KafkaListener(
            topics = "${app.kafka.topic.doc-uploaded}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void kafka(String message){
        System.out.println("Received: " + message);
    }

}
