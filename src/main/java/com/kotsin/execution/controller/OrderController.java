package com.kotsin.execution.controller;

import com.kotsin.execution.broker.BrokerException;
import com.kotsin.execution.broker.BrokerOrderService;
import com.kotsin.execution.service.WalletService;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.EqualsAndHashCode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
@Slf4j
public class OrderController {

    private final BrokerOrderService broker;
    private final WalletService wallet;

    @Value("${execution.mode:paper}")
    private String executionMode; // paper | live

    @PostMapping("/market")
    public ResponseEntity<?> market(@RequestBody MarketOrder req) {
        try {
            validate(req);
            String mode = executionMode;
            String orderId;
            if ("live".equalsIgnoreCase(mode)) {
                if (req.getExch() == null || req.getExchType() == null) {
                    return ResponseEntity.badRequest().body(Map.of("error", "exch and exchType are required in live mode"));
                }
                orderId = broker.placeMarketOrder(req.getScripCode(), req.getExch(), req.getExchType(),
                        "BUY".equalsIgnoreCase(req.getSide()) ? BrokerOrderService.Side.BUY : BrokerOrderService.Side.SELL,
                        req.getQty());
            } else {
                orderId = wallet.recordOrder(req.getScripCode(), req.getSide(), req.getQty(), null, mode);
            }
            var pos = wallet.updatePosition(req.getScripCode(), req.getSide(), req.getQty(), null);
            Map<String, Object> resp = new HashMap<>();
            resp.put("mode", mode);
            resp.put("orderId", orderId);
            resp.put("position", pos);
            return ResponseEntity.ok(resp);
        } catch (BrokerException be) {
            return ResponseEntity.status(502).body(Map.of("error", be.getMessage()));
        } catch (Exception e) {
            log.error("Order failed: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/limit")
    public ResponseEntity<?> limit(@RequestBody LimitOrder req) {
        try {
            validate(req);
            if (req.getPrice() == null || req.getPrice() <= 0) {
                return ResponseEntity.badRequest().body(Map.of("error", "price must be positive"));
            }
            String mode = executionMode;
            String orderId;
            if ("live".equalsIgnoreCase(mode)) {
                if (req.getExch() == null || req.getExchType() == null) {
                    return ResponseEntity.badRequest().body(Map.of("error", "exch and exchType are required in live mode"));
                }
                orderId = broker.placeLimitOrder(req.getScripCode(), req.getExch(), req.getExchType(),
                        "BUY".equalsIgnoreCase(req.getSide()) ? BrokerOrderService.Side.BUY : BrokerOrderService.Side.SELL,
                        req.getQty(), req.getPrice());
            } else {
                orderId = wallet.recordOrder(req.getScripCode(), req.getSide(), req.getQty(), req.getPrice(), mode);
            }
            var pos = wallet.updatePosition(req.getScripCode(), req.getSide(), req.getQty(), req.getPrice());
            Map<String, Object> resp = new HashMap<>();
            resp.put("mode", mode);
            resp.put("orderId", orderId);
            resp.put("position", pos);
            return ResponseEntity.ok(resp);
        } catch (BrokerException be) {
            return ResponseEntity.status(502).body(Map.of("error", be.getMessage()));
        } catch (Exception e) {
            log.error("Order failed: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    private void validate(BaseOrder req) {
        if (req.getScripCode() == null || req.getScripCode().isBlank())
            throw new IllegalArgumentException("scripCode required");
        if (req.getSide() == null || !(req.getSide().equalsIgnoreCase("BUY") || req.getSide().equalsIgnoreCase("SELL")))
            throw new IllegalArgumentException("side must be BUY or SELL");
        if (req.getQty() <= 0) throw new IllegalArgumentException("qty must be > 0");
    }

    @Data
    public static class BaseOrder {
        private String scripCode;
        private String side; // BUY | SELL
        private int qty;
        private String exch;
        private String exchType;
    }

    @Data
    @EqualsAndHashCode(callSuper = true)
    public static class MarketOrder extends BaseOrder {}

    @Data
    @EqualsAndHashCode(callSuper = true)
    public static class LimitOrder extends BaseOrder {
        private Double price;
    }
}

