package com.storex.inventory.model;

public record OrderCreatedEvent(String orderId, Long productId, int quantity) {
}

