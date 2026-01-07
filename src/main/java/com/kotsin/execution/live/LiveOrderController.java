package com.kotsin.execution.live;

import com.kotsin.execution.broker.BrokerException;
import com.kotsin.execution.broker.BrokerOrderService;
import com.kotsin.execution.broker.FivePaisaBrokerService;
import com.kotsin.execution.service.WalletService;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Live Order Controller - Handles real broker orders via 5Paisa API.
 *
 * This controller mirrors VirtualOrderController but executes real trades.
 * It manages:
 * - Order placement (market/limit)
 * - Position closing
 * - Position modification (SL/TP adjustments via internal tracking)
 *
 * Note: SL/TP/trailing are managed internally since 5Paisa doesn't support
 * bracket orders directly. The system tracks these and places exit orders
 * when conditions are met.
 */
@RestController
@RequestMapping("/api/live")
@RequiredArgsConstructor
@Slf4j
@CrossOrigin(origins = "*")
public class LiveOrderController {

    private final FivePaisaBrokerService brokerService;
    private final WalletService walletService;

    // Track live positions for SL/TP management
    private final ConcurrentHashMap<String, LivePosition> positions = new ConcurrentHashMap<>();

    /**
     * Create a new live order via broker.
     * Places order with 5Paisa and tracks position locally.
     */
    @PostMapping("/orders")
    public ResponseEntity<?> createOrder(@RequestBody CreateOrderRequest req) {
        log.info("📈 LIVE ORDER: scripCode={}, side={}, type={}, qty={}",
            req.scripCode, req.side, req.type, req.qty);

        try {
            // Validate required fields
            if (req.scripCode == null || req.scripCode.isBlank()) {
                return ResponseEntity.badRequest().body(Map.of("error", "scripCode is required"));
            }
            if (req.qty <= 0) {
                return ResponseEntity.badRequest().body(Map.of("error", "qty must be positive"));
            }

            // Parse scripCode - can be in format "N:C:18365" or just "18365"
            String numericScripCode;
            String exch;
            String exchType;

            if (req.scripCode.contains(":")) {
                // Format: "Exchange:ExchangeType:ScripCode" e.g., "N:C:18365"
                String[] parts = req.scripCode.split(":");
                if (parts.length >= 3) {
                    exch = parts[0];      // N, B, M
                    exchType = parts[1];  // C, D, U
                    numericScripCode = parts[2];
                    log.info("📊 Parsed scripCode: exch={}, exchType={}, scripCode={}", exch, exchType, numericScripCode);
                } else {
                    // Fallback if format is unexpected
                    numericScripCode = req.scripCode;
                    exch = req.exch != null ? req.exch : "N";
                    exchType = req.exchType != null ? req.exchType : "C";
                }
            } else {
                // Plain numeric scripCode
                numericScripCode = req.scripCode;
                exch = req.exch != null ? req.exch : "N";
                exchType = req.exchType != null ? req.exchType : "C";
            }

            BrokerOrderService.Side side = "BUY".equalsIgnoreCase(req.side)
                ? BrokerOrderService.Side.BUY
                : BrokerOrderService.Side.SELL;

            String orderId;
            double fillPrice = req.limitPrice != null ? req.limitPrice : 0;

            // Place order with broker using numeric scripCode
            if ("LIMIT".equalsIgnoreCase(req.type) && req.limitPrice != null && req.limitPrice > 0) {
                orderId = brokerService.placeLimitOrder(
                    numericScripCode, exch, exchType, side, req.qty, req.limitPrice);
                log.info("✅ LIMIT order placed: orderId={}, scripCode={}, price={}", orderId, numericScripCode, req.limitPrice);
            } else {
                orderId = brokerService.placeMarketOrder(
                    numericScripCode, exch, exchType, side, req.qty);
                log.info("✅ MARKET order placed: orderId={}, scripCode={}", orderId, numericScripCode);
            }

            // Record in wallet
            walletService.recordOrder(req.scripCode, req.side, req.qty, fillPrice, "LIVE");
            walletService.updatePosition(req.scripCode, req.side, req.qty, fillPrice);

            // Track position with SL/TP settings for internal management
            LivePosition pos = positions.computeIfAbsent(req.scripCode, k -> new LivePosition());
            pos.setScripCode(req.scripCode);
            pos.setSide(req.side);
            pos.setQty(pos.getQty() + ("BUY".equalsIgnoreCase(req.side) ? req.qty : -req.qty));
            pos.setExch(exch);
            pos.setExchType(exchType);
            pos.setSl(req.sl);
            pos.setTp1(req.tp1);
            pos.setTp2(req.tp2);
            pos.setTp1ClosePercent(req.tp1ClosePercent != null ? req.tp1ClosePercent : 50.0);
            pos.setTrailingType(req.trailingType);
            pos.setTrailingValue(req.trailingValue);
            pos.setLastOrderId(orderId);
            pos.setCreatedAt(System.currentTimeMillis());

            log.info("📊 Position tracked: scripCode={}, netQty={}, sl={}, tp1={}",
                req.scripCode, pos.getQty(), pos.getSl(), pos.getTp1());

            var response = new java.util.HashMap<String, Object>();
            response.put("orderId", orderId);
            response.put("scripCode", req.scripCode);
            response.put("side", req.side);
            response.put("type", req.type != null ? req.type : "MARKET");
            response.put("qty", req.qty);
            response.put("status", "PLACED");
            response.put("mode", "LIVE");
            response.put("sl", req.sl != null ? req.sl : 0);
            response.put("tp1", req.tp1 != null ? req.tp1 : 0);
            response.put("tp2", req.tp2 != null ? req.tp2 : 0);
            response.put("ts", System.currentTimeMillis());
            return ResponseEntity.ok(response);

        } catch (BrokerException e) {
            log.error("❌ Broker error placing order: {}", e.getMessage(), e);
            return ResponseEntity.status(500).body(Map.of(
                "error", "Broker order failed",
                "message", e.getMessage(),
                "mode", "LIVE"
            ));
        } catch (Exception e) {
            log.error("❌ Error creating live order: {}", e.getMessage(), e);
            return ResponseEntity.status(500).body(Map.of(
                "error", e.getMessage(),
                "mode", "LIVE"
            ));
        }
    }

    /**
     * Close an existing live position.
     * Places opposite order to square off the position.
     */
    @PostMapping("/close/{scripCode}")
    public ResponseEntity<?> closePosition(@PathVariable String scripCode) {
        log.info("🔒 CLOSE LIVE POSITION: scripCode={}", scripCode);

        try {
            LivePosition pos = positions.get(scripCode);
            if (pos == null || pos.getQty() == 0) {
                log.warn("No live position found for {}", scripCode);
                return ResponseEntity.status(404).body(Map.of(
                    "error", "No position found",
                    "scripCode", scripCode,
                    "mode", "LIVE"
                ));
            }

            // Determine exit side (opposite of position)
            BrokerOrderService.Side exitSide = pos.getQty() > 0
                ? BrokerOrderService.Side.SELL
                : BrokerOrderService.Side.BUY;
            int qtyToClose = Math.abs(pos.getQty());

            // Place exit order
            String orderId = brokerService.placeMarketOrder(
                scripCode, pos.getExch(), pos.getExchType(), exitSide, qtyToClose);

            log.info("✅ Position closed: orderId={}, qty={}, side={}", orderId, qtyToClose, exitSide);

            // Update wallet
            String closeSide = exitSide == BrokerOrderService.Side.BUY ? "BUY" : "SELL";
            walletService.recordOrder(scripCode, closeSide, qtyToClose, null, "LIVE_CLOSE");
            walletService.updatePosition(scripCode, closeSide, qtyToClose, null);

            // Clear tracked position
            positions.remove(scripCode);

            return ResponseEntity.ok(Map.of(
                "orderId", orderId,
                "scripCode", scripCode,
                "action", "CLOSED",
                "qtyClosed", qtyToClose,
                "mode", "LIVE",
                "ts", System.currentTimeMillis()
            ));

        } catch (BrokerException e) {
            log.error("❌ Broker error closing position: {}", e.getMessage(), e);
            return ResponseEntity.status(500).body(Map.of(
                "error", "Broker close failed",
                "message", e.getMessage(),
                "mode", "LIVE"
            ));
        } catch (Exception e) {
            log.error("❌ Error closing position: {}", e.getMessage(), e);
            return ResponseEntity.status(500).body(Map.of(
                "error", e.getMessage(),
                "mode", "LIVE"
            ));
        }
    }

    /**
     * Modify position parameters (SL, TP, trailing).
     * These are tracked internally for automated exit management.
     * Note: Actual broker orders are placed when conditions trigger.
     */
    @PatchMapping("/positions/{scripCode}")
    public ResponseEntity<?> modifyPosition(@PathVariable String scripCode, @RequestBody ModifyPositionRequest req) {
        log.info("📝 MODIFY LIVE POSITION: scripCode={}", scripCode);

        LivePosition pos = positions.get(scripCode);
        if (pos == null) {
            return ResponseEntity.status(404).body(Map.of(
                "error", "No position found",
                "scripCode", scripCode,
                "mode", "LIVE"
            ));
        }

        // Update tracked parameters
        if (req.sl != null) {
            pos.setSl(req.sl);
            log.info("Updated SL to {} for {}", req.sl, scripCode);
        }
        if (req.tp1 != null) {
            pos.setTp1(req.tp1);
            log.info("Updated TP1 to {} for {}", req.tp1, scripCode);
        }
        if (req.tp2 != null) {
            pos.setTp2(req.tp2);
            log.info("Updated TP2 to {} for {}", req.tp2, scripCode);
        }
        if (req.tp1ClosePercent != null) {
            pos.setTp1ClosePercent(req.tp1ClosePercent);
        }
        if (req.trailingType != null) {
            pos.setTrailingType(req.trailingType);
        }
        if (req.trailingValue != null) {
            pos.setTrailingValue(req.trailingValue);
        }
        if (req.trailingActive != null) {
            pos.setTrailingActive(req.trailingActive);
        }
        pos.setUpdatedAt(System.currentTimeMillis());

        return ResponseEntity.ok(Map.of(
            "scripCode", scripCode,
            "qty", pos.getQty(),
            "side", pos.getSide(),
            "sl", pos.getSl() != null ? pos.getSl() : 0,
            "tp1", pos.getTp1() != null ? pos.getTp1() : 0,
            "tp2", pos.getTp2() != null ? pos.getTp2() : 0,
            "trailingType", pos.getTrailingType() != null ? pos.getTrailingType() : "NONE",
            "mode", "LIVE",
            "updatedAt", pos.getUpdatedAt()
        ));
    }

    /**
     * Health check endpoint.
     */
    @GetMapping("/health")
    public Map<String, Object> health() {
        return Map.of(
            "status", "UP",
            "mode", "LIVE",
            "activePositions", positions.size(),
            "ts", System.currentTimeMillis()
        );
    }

    /**
     * Get all tracked live positions.
     */
    @GetMapping("/positions")
    public ResponseEntity<?> getPositions() {
        return ResponseEntity.ok(Map.of(
            "positions", positions.values(),
            "count", positions.size(),
            "mode", "LIVE",
            "ts", System.currentTimeMillis()
        ));
    }

    // ========== DTOs ==========

    @Data
    public static class CreateOrderRequest {
        public String scripCode;
        public String side;      // BUY or SELL
        public String type;      // MARKET or LIMIT
        public int qty;
        public Double limitPrice;
        public Double sl;
        public Double tp1;
        public Double tp2;
        public Double tp1ClosePercent;
        public String trailingType;
        public Double trailingValue;
        public Double trailingStep;
        // Optional exchange params (default to NSE Cash)
        public String exch;      // N=NSE, B=BSE, M=MCX
        public String exchType;  // C=Cash, D=Derivatives
    }

    @Data
    public static class ModifyPositionRequest {
        public Double sl;
        public Double tp1;
        public Double tp2;
        public Double tp1ClosePercent;
        public String trailingType;
        public Double trailingValue;
        public Double trailingStep;
        public Boolean trailingActive;
    }

    @Data
    public static class LivePosition {
        private String scripCode;
        private String side;
        private int qty;
        private String exch = "N";
        private String exchType = "C";
        private Double avgPrice;
        private Double sl;
        private Double tp1;
        private Double tp2;
        private Double tp1ClosePercent = 50.0;
        private boolean tp1Hit = false;
        private String trailingType;
        private Double trailingValue;
        private boolean trailingActive = false;
        private String lastOrderId;
        private long createdAt;
        private long updatedAt;
    }
}
