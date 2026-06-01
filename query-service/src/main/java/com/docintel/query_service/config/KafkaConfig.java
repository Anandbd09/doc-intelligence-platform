package com.docintel.query_service.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    @Value("${app.kafka.topic.query-executed}")
    private String queryExecuted;

    @Bean
    public NewTopic queryExecuted(){
        return TopicBuilder.name(queryExecuted)
                .partitions(1)
                .replicas(1)
                .build();
    }
}
