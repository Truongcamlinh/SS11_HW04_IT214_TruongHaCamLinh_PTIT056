package com.storex.inventory.config;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaErrorHandlerConfig {
    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlerConfig.class);
    static final long RETRY_INTERVAL_MS = 2_000L;
    static final long RETRY_ATTEMPTS = 3L;

    @Bean
    DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(
            KafkaOperations<Object, Object> kafkaTemplate) {
        return new DeadLetterPublishingRecoverer(kafkaTemplate, (record, exception) -> {
            log.error("Đã ném đơn hàng bị lỗi vào DLQ: topic={}, partition={}, offset={}, lỗi={}",
                    record.topic(), record.partition(), record.offset(), exception.getMessage());
            return new TopicPartition("storex-order-events.DLQ", record.partition());
        });
    }

    @Bean
    DefaultErrorHandler kafkaErrorHandler(DeadLetterPublishingRecoverer recoverer) {
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                recoverer,
                new FixedBackOff(RETRY_INTERVAL_MS, RETRY_ATTEMPTS));

        errorHandler.setRetryListeners((record, exception, deliveryAttempt) ->
                log.warn("Xử lý thất bại, chuẩn bị retry lần {} sau 2 giây: "
                                + "partition={}, offset={}, lỗi={}",
                        deliveryAttempt, record.partition(), record.offset(),
                        exception.getMessage()));
        errorHandler.setCommitRecovered(true);
        return errorHandler;
    }
}

