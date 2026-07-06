package dev.rafee.orders.ingest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.time.Instant;
import java.util.Map;

/**
 * Hot path: validate, assign a ULID, publish to Kafka, return 202.
 * Deliberately no database write here — persistence is the worker's job.
 * Keyed by storeId so each store's orders stay in one partition (ordered).
 */
@RestController
public class OrderController {

    public record OrderRequest(
            @NotBlank @Size(max = 40) String storeId,
            @NotNull @Valid Customer customer,
            @NotEmpty @Size(max = 40) java.util.List<@Valid Line> lines,
            @Min(0) @Max(10_000_000) int totalCents) {
    }

    public record Customer(@NotBlank @Size(max = 80) String name, @Size(max = 30) String phone) {
    }

    public record Line(@NotBlank @Size(max = 120) String name, @Min(1) @Max(99) int quantity,
                       @Min(0) @Max(1_000_000) int unitPriceCents) {
    }

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;

    public OrderController(KafkaTemplate<String, String> kafka, ObjectMapper mapper) {
        this.kafka = kafka;
        this.mapper = mapper;
    }

    @ExceptionHandler({ java.util.concurrent.TimeoutException.class, java.util.concurrent.ExecutionException.class })
    public ResponseEntity<Map<String, String>> brokerUnavailable(Exception e) {
        // Honest failure: if durability can't be confirmed, say so and let
        // the client retry — never a 202 that might be a lie.
        return ResponseEntity.status(503).body(Map.of("error", "order intake temporarily unavailable — retry"));
    }

    private static final java.util.regex.Pattern IDEM_KEY = java.util.regex.Pattern.compile("[A-Za-z0-9_-]{8,64}");

    @PostMapping("/orders")
    public ResponseEntity<Map<String, String>> create(
            @Valid @RequestBody OrderRequest req,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey) throws Exception {
        // A timed-out request is *indeterminate*: the broker-restart chaos
        // test produced 15 orders that were 503'd to the client yet still
        // persisted once the producer's buffer flushed after recovery. A
        // client that retries must therefore reuse an Idempotency-Key —
        // the key becomes the order id, so the retry lands on the same
        // row and ON CONFLICT makes it a no-op instead of a double order.
        String id = (idempotencyKey != null && IDEM_KEY.matcher(idempotencyKey).matches())
                ? "IK-" + idempotencyKey
                : Ulid.next();
        ObjectNode event = mapper.createObjectNode();
        event.put("id", id);
        event.put("storeId", req.storeId());
        event.put("createdAt", Instant.now().toString());
        event.put("totalCents", req.totalCents());
        event.set("customer", mapper.valueToTree(req.customer()));
        event.set("lines", (JsonNode) mapper.valueToTree(req.lines()));

        // Await the broker ack (acks=all) before answering: a 202 is a
        // durability promise. Without this, orders buffered client-side
        // during a broker outage would be 202'd and then silently dropped
        // when delivery.timeout.ms expired — found designing the
        // broker-restart chaos test. Bounded wait so an outage turns into
        // fast 503s instead of hung connections.
        kafka.send("orders.v1", req.storeId(), mapper.writeValueAsString(event))
                .get(5, java.util.concurrent.TimeUnit.SECONDS);

        return ResponseEntity.accepted()
                .location(URI.create("/orders/" + id))
                .body(Map.of("id", id, "status", "accepted"));
    }
}
