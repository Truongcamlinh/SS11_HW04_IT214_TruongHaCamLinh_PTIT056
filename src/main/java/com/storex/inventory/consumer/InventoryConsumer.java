package com.storex.inventory.consumer;

import com.storex.inventory.model.OrderCreatedEvent;
import com.storex.inventory.service.InventoryService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class InventoryConsumer {
    private static final Logger log = LoggerFactory.getLogger(InventoryConsumer.class);

    private final InventoryService inventoryService;

    public InventoryConsumer(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    @KafkaListener(topics = "${storex.kafka.order-topic}")
    public void consume(ConsumerRecord<String, OrderCreatedEvent> record) {
        OrderCreatedEvent event = record.value();
        log.info("Nhận đơn hàng {}, partition={}, offset={}",
                event.orderId(), record.partition(), record.offset());
        inventoryService.deductStock(event);
    }
}

