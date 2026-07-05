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
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
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

    @PostMapping("/orders")
    public ResponseEntity<Map<String, String>> create(@Valid @RequestBody OrderRequest req) throws Exception {
        String id = Ulid.next();
        ObjectNode event = mapper.createObjectNode();
        event.put("id", id);
        event.put("storeId", req.storeId());
        event.put("createdAt", Instant.now().toString());
        event.put("totalCents", req.totalCents());
        event.set("customer", mapper.valueToTree(req.customer()));
        event.set("lines", (JsonNode) mapper.valueToTree(req.lines()));

        kafka.send("orders.v1", req.storeId(), mapper.writeValueAsString(event));

        return ResponseEntity.accepted()
                .location(URI.create("/orders/" + id))
                .body(Map.of("id", id, "status", "accepted"));
    }
}
