package dev.rafee.orders.ingest;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(OrderController.class)
class OrderControllerTest {

    @Autowired
    MockMvc mvc;

    @MockitoBean
    KafkaTemplate<String, String> kafka;

    private static final String VALID = """
            {"storeId":"waluigis","customer":{"name":"Ada","phone":"5550100000"},
             "lines":[{"name":"Pie","quantity":1,"unitPriceCents":1799}],"totalCents":1799}
            """;

    private void brokerAcks() {
        when(kafka.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture((SendResult<String, String>) null));
    }

    @Test
    void validOrderIsAcceptedWithUlidAndLocation() throws Exception {
        brokerAcks();
        mvc.perform(post("/orders").contentType("application/json").content(VALID))
                .andExpect(status().isAccepted())
                .andExpect(header().exists("Location"))
                .andExpect(jsonPath("$.id").value(org.hamcrest.Matchers.matchesPattern("[0-9A-HJKMNP-TV-Z]{26}")));
        verify(kafka).send(anyString(), anyString(), anyString());
    }

    @Test
    void idempotencyKeyBecomesTheOrderId() throws Exception {
        brokerAcks();
        mvc.perform(post("/orders").contentType("application/json").content(VALID)
                        .header("Idempotency-Key", "retry-abc-12345"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.id").value("IK-retry-abc-12345"));
    }

    @Test
    void malformedIdempotencyKeyFallsBackToUlid() throws Exception {
        brokerAcks();
        mvc.perform(post("/orders").contentType("application/json").content(VALID)
                        .header("Idempotency-Key", "too short"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.id").value(org.hamcrest.Matchers.matchesPattern("[0-9A-HJKMNP-TV-Z]{26}")));
    }

    @Test
    void invalidOrderIsRejectedWithoutTouchingKafka() throws Exception {
        mvc.perform(post("/orders").contentType("application/json")
                        .content("{\"storeId\":\"\",\"lines\":[],\"totalCents\":-5}"))
                .andExpect(status().isBadRequest());
        org.mockito.Mockito.verifyNoInteractions(kafka);
    }

    @Test
    void brokerTimeoutSurfacesAs503NotFake202() throws Exception {
        when(kafka.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));
        mvc.perform(post("/orders").contentType("application/json").content(VALID))
                .andExpect(status().isServiceUnavailable());
    }
}
