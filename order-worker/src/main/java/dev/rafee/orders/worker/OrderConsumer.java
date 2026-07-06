package dev.rafee.orders.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Consumes orders.v1 in batches and persists them with one JDBC batch
 * statement per poll. Delivery is at-least-once (rebalances and retries
 * redeliver whole batches), so every row is ON CONFLICT DO NOTHING keyed
 * on the ULID assigned at ingest — replays are no-ops, never duplicates.
 *
 * Batching is the fix for the measured bottleneck: single-row inserts
 * capped persistence at ~500 rows/s while ingest sustained ~5,250/s
 * (see docs/FAILURE_MODES.md). One multi-row batch per poll amortizes
 * the network round-trip and WAL sync across up to max-poll-records
 * rows.
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
    private final DashboardWebSocket dashboard;

    public OrderConsumer(JdbcTemplate jdbc, ObjectMapper mapper, DashboardWebSocket dashboard) {
        this.jdbc = jdbc;
        this.mapper = mapper;
        this.dashboard = dashboard;
    }

    @KafkaListener(topics = "orders.v1", groupId = "order-workers", batch = "true")
    public void onOrders(List<String> messages) throws Exception {
        List<Object[]> rows = new ArrayList<>(messages.size());
        for (String message : messages) {
            JsonNode event = mapper.readTree(message);
            rows.add(new Object[] {
                    event.path("id").asText(),
                    event.path("storeId").asText(),
                    event.path("customer").toString(),
                    event.path("lines").toString(),
                    event.path("totalCents").asInt(),
                    OffsetDateTime.parse(event.path("createdAt").asText()),
            });
        }
        int[] results = jdbc.batchUpdate(UPSERT, rows);
        int duplicates = 0;
        for (int r : results) {
            if (r == 0) duplicates++;
        }
        if (duplicates > 0) {
            log.info("batch of {}: {} duplicate deliveries ignored", rows.size(), duplicates);
        }
        // After the batch commits: the dashboard shows durable orders only.
        dashboard.broadcast("[" + String.join(",", messages) + "]");
    }
}
