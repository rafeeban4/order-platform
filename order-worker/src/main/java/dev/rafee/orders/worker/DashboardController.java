package dev.rafee.orders.worker;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/** Backfill + stats for the live dashboard (static/index.html). */
@RestController
public class DashboardController {

    private final JdbcTemplate jdbc;

    public DashboardController(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @GetMapping("/orders/recent")
    public List<Map<String, Object>> recent(@RequestParam(defaultValue = "30") int limit) {
        return jdbc.queryForList("""
                SELECT id, store_id, status, total_cents, created_at,
                       customer->>'name' AS customer_name
                FROM orders ORDER BY created_at DESC LIMIT ?
                """, Math.min(Math.max(limit, 1), 200));
    }

    @GetMapping("/orders/stats")
    public Map<String, Object> stats() {
        return jdbc.queryForMap("""
                SELECT count(*) AS total_orders,
                       coalesce(sum(total_cents), 0) AS total_cents,
                       count(*) FILTER (WHERE created_at > now() - interval '60 seconds') AS last_minute
                FROM orders
                """);
    }
}
