package dev.rafee.orders.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;

/**
 * Consumes orders.v1 and persists idempotently. Delivery is at-least-once
 * (rebalances and retries can redeliver), so the insert is ON CONFLICT DO
 * NOTHING keyed on the ULID assigned at ingest — replaying a message is a
 * no-op instead of a duplicate row. That is the whole consistency story:
 * at-least-once delivery + idempotent writes, no exactly-once machinery.
 */
@Component
public class OrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    private static final String UPSERT = """
            INSERT INTO orders (id, store_id, status, customer, lines, total_cents, created_at)
            VALUES (?, ?, 'new', ?::jsonb, ?::jsonb, ?, ?)
            ON CONFLICT (id) DO NOTHING
            """;

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;

    public OrderConsumer(JdbcTemplate jdbc, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.mapper = mapper;
    }

    @KafkaListener(topics = "orders.v1", groupId = "order-workers")
    public void onOrder(String message) throws Exception {
        JsonNode event = mapper.readTree(message);
        int updated = jdbc.update(UPSERT,
                event.path("id").asText(),
                event.path("storeId").asText(),
                event.path("customer").toString(),
                event.path("lines").toString(),
                event.path("totalCents").asInt(),
                OffsetDateTime.parse(event.path("createdAt").asText()));
        if (updated == 0) {
            log.info("duplicate delivery ignored: {}", event.path("id").asText());
        }
    }
}
