package com.storex.inventory.service;

import com.storex.inventory.model.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class InventoryService {
    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);

    public void deductStock(OrderCreatedEvent event) {
        if (event.productId() == null) {
            throw new IllegalArgumentException("productId không được để trống");
        }
        if (event.quantity() <= 0) {
            throw new IllegalArgumentException("quantity phải lớn hơn 0");
        }

        log.info("Đã trừ {} sản phẩm {} cho đơn hàng {}",
                event.quantity(), event.productId(), event.orderId());
    }
}

