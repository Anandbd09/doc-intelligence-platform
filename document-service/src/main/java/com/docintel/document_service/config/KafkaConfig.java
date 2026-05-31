package com.docintel.document_service.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    @Value("${app.kafka.topic.doc-uploaded}")
    private String docUploadedTopic;

    @Bean
    public NewTopic docUploadedTopic(){
        return TopicBuilder.name(docUploadedTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }
}
