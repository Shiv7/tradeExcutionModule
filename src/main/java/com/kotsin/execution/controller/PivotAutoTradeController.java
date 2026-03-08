package com.kotsin.execution.controller;

import com.kotsin.execution.consumer.PivotConfluenceSignalConsumer;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * REST controller for Pivot Confluence auto-trade pause/resume.
 * Used by dashboard frontend to toggle auto-trading from the Pivot tab.
 */
@RestController
@RequestMapping("/api/pivot")
@RequiredArgsConstructor
@CrossOrigin(origins = "*")
public class PivotAutoTradeController {

    private final PivotConfluenceSignalConsumer pivotConsumer;

    @GetMapping("/auto-trade/status")
    public ResponseEntity<Map<String, Object>> getStatus() {
        boolean paused = pivotConsumer.isPaused();
        return ResponseEntity.ok(Map.of(
                "autoTradeEnabled", !paused,
                "paused", paused
        ));
    }

    @PostMapping("/auto-trade/toggle")
    public ResponseEntity<Map<String, Object>> toggle(@RequestBody(required = false) Map<String, Object> body) {
        boolean currentlyPaused = pivotConsumer.isPaused();
        boolean newPaused;

        if (body != null && body.containsKey("enabled")) {
            newPaused = !Boolean.parseBoolean(body.get("enabled").toString());
        } else {
            newPaused = !currentlyPaused;
        }

        pivotConsumer.setPaused(newPaused);

        return ResponseEntity.ok(Map.of(
                "autoTradeEnabled", !newPaused,
                "paused", newPaused
        ));
    }
}
