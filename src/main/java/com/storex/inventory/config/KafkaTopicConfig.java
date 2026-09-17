package com.storex.inventory.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {
    @Bean
    NewTopic orderEventsTopic() {
        return TopicBuilder.name("storex-order-events")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    NewTopic deadLetterTopic() {
        return TopicBuilder.name("storex-order-events.DLQ")
                .partitions(3)
                .replicas(1)
                .build();
    }
}

