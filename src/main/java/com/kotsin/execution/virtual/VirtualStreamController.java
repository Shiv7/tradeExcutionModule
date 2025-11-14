package com.kotsin.execution.virtual;

import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/virtual")
@RequiredArgsConstructor
@CrossOrigin(origins = "*")
public class VirtualStreamController {
    private final VirtualEventBus bus;
    private final VirtualWalletRepository repo;

    @GetMapping(path = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream(){
        SseEmitter emitter = bus.subscribe();
        // Send initial wallet snapshot (orders + positions)
        try {
            Map<String, Object> snapshot = new HashMap<>();
            snapshot.put("orders", repo.listOrders(200));
            snapshot.put("positions", repo.listPositions());
            snapshot.put("timestamp", System.currentTimeMillis());
            emitter.send(SseEmitter.event().name("wallet").data(snapshot));
        } catch (IOException ignore) {}
        return emitter;
    }
}
