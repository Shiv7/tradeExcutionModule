package com.kotsin.execution.virtual;

import com.kotsin.execution.virtual.model.VirtualOrder;
import com.kotsin.execution.virtual.model.VirtualPosition;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/virtual")
@RequiredArgsConstructor
@Slf4j
@org.springframework.web.bind.annotation.CrossOrigin(origins = "*")
public class VirtualOrderController {
    private final VirtualEngineService engine;
    private final VirtualWalletRepository repo;
    private final VirtualEventBus bus;

    @PostMapping("/orders")
    public ResponseEntity<?> create(@RequestBody CreateOrder req){
        try {
            VirtualOrder o = new VirtualOrder();
            o.setScripCode(req.scripCode);
            o.setSide("BUY".equalsIgnoreCase(req.side) ? VirtualOrder.Side.BUY : VirtualOrder.Side.SELL);
            o.setType("LIMIT".equalsIgnoreCase(req.type) ? VirtualOrder.Type.LIMIT : VirtualOrder.Type.MARKET);
            o.setQty(req.qty);
            o.setLimitPrice(req.limitPrice);
            o.setSl(req.sl);
            o.setTp1(req.tp1);
            o.setTp2(req.tp2);
            o.setTp1ClosePercent(req.tp1ClosePercent);
            o.setTrailingType(req.trailingType);
            o.setTrailingValue(req.trailingValue);
            o.setTrailingStep(req.trailingStep);
            VirtualOrder saved = engine.createOrder(o);
            return ResponseEntity.ok(saved);
        } catch (Exception e){
            log.error("Virtual order create failed: {}", e.getMessage(), e);
            return ResponseEntity.status(500).body(Map.of("error", e.getMessage()));
        }
    }

    // Removed legacy endpoints: listOrders, listPositions (use SSE /api/virtual/stream instead)

    @PostMapping("/close/{scripCode}")
    public ResponseEntity<?> close(@PathVariable String scripCode){
        return engine.closePosition(scripCode)
                .<ResponseEntity<?>>map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.status(404).body(Map.of("error","no position")));
    }

    @GetMapping("/health")
    public Map<String,Object> health(){
        return Map.of("status","UP","ts",System.currentTimeMillis());
    }

    @PatchMapping("/positions/{scripCode}")
    public ResponseEntity<?> modify(@PathVariable String scripCode, @RequestBody ModifyPosition req){
        var posOpt = repo.getPosition(scripCode);
        if (posOpt.isEmpty()) return ResponseEntity.status(404).body(Map.of("error","no position"));
        var p = posOpt.get();
        if (req.sl != null) p.setSl(req.sl);
        if (req.tp1 != null) { p.setTp1(req.tp1); p.setTp1Hit(Boolean.FALSE); }
        if (req.tp2 != null) p.setTp2(req.tp2);
        if (req.tp1ClosePercent != null) p.setTp1ClosePercent(req.tp1ClosePercent);
        if (req.trailingType != null) p.setTrailingType(req.trailingType);
        if (req.trailingValue != null) p.setTrailingValue(req.trailingValue);
        if (req.trailingStep != null) p.setTrailingStep(req.trailingStep);
        if (req.trailingActive != null) p.setTrailingActive(req.trailingActive);
        p.setUpdatedAt(System.currentTimeMillis());
        repo.savePosition(p);
        try { bus.publish("position.updated", p); } catch (Exception ignore) {}
        return ResponseEntity.ok(p);
    }

    @Data
    public static class CreateOrder {
        public String scripCode; public String side; public String type;
        public int qty; public Double limitPrice; public Double sl; public Double tp1; public Double tp2; public Double tp1ClosePercent;
        public String trailingType; public Double trailingValue; public Double trailingStep;
    }

    @lombok.Data
    public static class ModifyPosition {
        public Double sl; public Double tp1; public Double tp2; public Double tp1ClosePercent;
        public String trailingType; public Double trailingValue; public Double trailingStep; public Boolean trailingActive;
    }
}
