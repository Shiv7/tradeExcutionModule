package com.kotsin.execution.virtual;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
@Slf4j
public class VirtualEventBus {
    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    public SseEmitter subscribe(){
        SseEmitter emitter = new SseEmitter(0L); // no timeout
        emitters.add(emitter);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        try { emitter.send(SseEmitter.event().name("hello").data("ok")); } catch (IOException e) { log.debug("Hello ping failed: {}", e.getMessage()); }
        return emitter;
    }

    public void publish(String event, Object data){
        for (SseEmitter e: emitters){
            try {
                e.send(SseEmitter.event().name(event).data(data));
            } catch (Exception ex){ emitters.remove(e); }
        }
    }
}

