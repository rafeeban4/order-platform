package dev.rafee.orders.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Raw WebSocket fan-out of persisted orders to any connected dashboard.
 * Deliberately fire-and-forget: a slow or dead dashboard must never block
 * the persistence path, so failed sends just drop the session. Broadcasts
 * happen AFTER the JDBC batch commits — the screen only ever shows
 * durable truth, not in-flight messages.
 */
@Component
public class DashboardWebSocket extends TextWebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(DashboardWebSocket.class);
    private final Set<WebSocketSession> sessions = new CopyOnWriteArraySet<>();

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        sessions.add(session);
        log.info("dashboard connected ({} active)", sessions.size());
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        sessions.remove(session);
    }

    public void broadcast(String json) {
        TextMessage msg = new TextMessage(json);
        for (WebSocketSession session : sessions) {
            try {
                session.sendMessage(msg);
            } catch (IOException | IllegalStateException e) {
                sessions.remove(session);
            }
        }
    }

    @Configuration
    @EnableWebSocket
    static class Config implements WebSocketConfigurer {
        private final DashboardWebSocket handler;

        Config(DashboardWebSocket handler) {
            this.handler = handler;
        }

        @Override
        public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
            registry.addHandler(handler, "/ws").setAllowedOrigins("*");
        }
    }
}
