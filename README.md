# SS11 HW04 - Chống chịu lỗi và Dead Letter Queue

**Sinh viên:** Trương Hà Cẩm Linh

**Lớp:** IT214

**Mã sinh viên:** PTIT056

## 1. Bài toán

Inventory Service đọc sự kiện đơn hàng từ topic `storex-order-events`. Một message có JSON sai hoặc có `productId: null` có thể làm consumer ném ngoại lệ. Nếu không có cơ chế xử lý lỗi, consumer tiếp tục đọc lại cùng offset, khiến partition bị chặn và các đơn hàng hợp lệ phía sau không được xử lý.

Bài làm sử dụng `DefaultErrorHandler`, `FixedBackOff` và `DeadLetterPublishingRecoverer` để giới hạn số lần thử lại và chuyển message không thể xử lý sang Dead Letter Queue.

## 2. Luồng xử lý

```text
storex-order-events
        |
        v
Inventory Consumer
        |
        |-- thành công -> ghi nhận offset -> đọc message tiếp theo
        |
        `-- thất bại -> chờ 2 giây -> retry tối đa 3 lần
                                      |
                                      `-- vẫn lỗi -> storex-order-events.DLQ
```

`FixedBackOff(2000, 3)` tạo khoảng nghỉ 2 giây và tối đa 3 lần retry sau lần xử lý ban đầu. Khi cả ba lần đều thất bại, recoverer gửi message sang DLQ. Sau khi recovery thành công, offset được commit để consumer tiếp tục xử lý message tiếp theo.

## 3. Lỗi JSON và BUG-05

Nếu dùng trực tiếp `JsonDeserializer`, lỗi JSON xảy ra trước khi phương thức `@KafkaListener` được gọi. `ErrorHandlingDeserializer` được đặt bên ngoài để đóng gói lỗi giải mã và chuyển ngoại lệ cho error handler xử lý.

Spring Kafka chỉ giải mã các lớp thuộc trusted packages nhằm hạn chế việc tạo đối tượng Java không an toàn từ dữ liệu bên ngoài. Khi DTO do service khác gửi không nằm trong danh sách tin cậy, consumer báo lỗi:

```text
The class is not in the trusted packages
```

Theo yêu cầu BUG-05, cấu hình đã bổ sung:

```yaml
spring:
  kafka:
    consumer:
      value-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
      properties:
        spring.deserializer.value.delegate.class: org.springframework.kafka.support.serializer.JsonDeserializer
        spring.json.trusted.packages: "*"
```

Trong hệ thống thực tế nên giới hạn package cụ thể thay vì `*`. Bài này sử dụng `*` đúng theo yêu cầu đề bài.

## 4. Lỗi nghiệp vụ productId null

JSON có thể hợp lệ nhưng dữ liệu không hợp lệ. `InventoryService` chủ động kiểm tra:

```java
if (event.productId() == null) {
    throw new IllegalArgumentException("productId không được để trống");
}
```

Ngoại lệ được truyền về Kafka listener container. `DefaultErrorHandler` giữ consumer ở offset hiện tại, chờ 2 giây rồi gọi lại listener. Sau 3 lần retry, message được chuyển sang DLQ.

## 5. Cấu hình Retry và DLQ

Phần cấu hình chính nằm tại:

```text
src/main/java/com/storex/inventory/config/KafkaErrorHandlerConfig.java
```

```java
new FixedBackOff(2_000L, 3L)
```

DLQ có tên chính xác:

```text
storex-order-events.DLQ
```

Topic chính và DLQ đều được tạo với 3 partition. Giữ cùng số partition giúp recoverer có thể chuyển message lỗi sang partition tương ứng.

## 6. Ghi log theo REQ-02

Recoverer chỉ chạy sau khi đã hết số lần retry. Tại thời điểm đó hệ thống ghi log mức `ERROR`:

```text
Đã ném đơn hàng bị lỗi vào DLQ
```

Log có thêm topic, partition, offset và nguyên nhân để hỗ trợ điều tra. Mức `ERROR` thường được console hiển thị màu đỏ nếu terminal hỗ trợ màu log.

## 7. Kiểm thử thủ công

Khởi động Kafka tại `localhost:9092`, sau đó chạy:

```bash
./gradlew bootRun
```

Gửi một message hợp lệ:

```json
{"orderId":"ORD-001","productId":101,"quantity":2}
```

Gửi message lỗi nghiệp vụ:

```json
{"orderId":"ORD-002","productId":null,"quantity":2}
```

Hoặc gửi JSON sai định dạng:

```text
{"orderId":"ORD-003","productId":101
```

Kết quả mong đợi: consumer thử lại 3 lần, mỗi lần cách nhau 2 giây. Sau đó log `ERROR` xuất hiện, message được đưa vào `storex-order-events.DLQ`, còn partition chính tiếp tục xử lý message phía sau.

Có thể đọc DLQ bằng Kafka CLI:

```bash
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic storex-order-events.DLQ --from-beginning
```

## 8. Build và chạy kiểm thử tự động

```bash
./gradlew clean build
```

Test tự động xác nhận sự kiện hợp lệ được xử lý và sự kiện thiếu `productId` ném đúng ngoại lệ để kích hoạt error handler.
