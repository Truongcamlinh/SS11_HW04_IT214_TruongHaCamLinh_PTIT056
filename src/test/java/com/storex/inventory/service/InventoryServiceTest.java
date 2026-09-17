package com.storex.inventory.service;

import com.storex.inventory.model.OrderCreatedEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class InventoryServiceTest {
    private final InventoryService inventoryService = new InventoryService();

    @Test
    void rejectsEventWithoutProductId() {
        OrderCreatedEvent event = new OrderCreatedEvent("ORD-ERROR", null, 2);

        assertThrows(IllegalArgumentException.class,
                () -> inventoryService.deductStock(event));
    }

    @Test
    void acceptsValidEvent() {
        OrderCreatedEvent event = new OrderCreatedEvent("ORD-OK", 101L, 2);

        assertDoesNotThrow(() -> inventoryService.deductStock(event));
    }
}

