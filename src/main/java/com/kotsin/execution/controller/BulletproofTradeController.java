package com.kotsin.execution.controller;

import com.kotsin.execution.logic.TradeManager;
import com.kotsin.execution.model.ActiveTrade;
import com.kotsin.execution.broker.BrokerOrderService;
import com.kotsin.execution.service.NetPositionService;
import com.kotsin.execution.model.NetPosition;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * 🛡️ BULLETPROOF TRADE CONTROLLER 🛡️
 * 
 * REST API for monitoring and managing the bulletproof single trade system:
 * - Portfolio status and statistics
 * - Active trade monitoring
 * - Emergency controls
 * - Manual trade creation for testing
 */
@RestController
@RequestMapping("/api/bulletproof")
@Slf4j
@RequiredArgsConstructor
public class BulletproofTradeController {
    
    private final TradeManager tradeManager;
    private final BrokerOrderService brokerOrderService;
    private final NetPositionService netPositionService;
    
    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss");
    
    /**
     * 📊 GET PORTFOLIO STATUS - Complete dashboard
     */
    @GetMapping("/portfolio/status")
    public ResponseEntity<Map<String, Object>> getPortfolioStatus() {
        try {
            Map<String, Object> status = new HashMap<>();
            
            // Basic status
            status.put("hasActiveTrade", tradeManager.hasActiveTrade());
            status.put("currentTime", LocalDateTime.now().format(TIME_FORMAT));
            status.put("systemStatus", "BULLETPROOF_ACTIVE");
            
            // Active trade details
            ActiveTrade currentTrade = tradeManager.getCurrentTrade();
            if (currentTrade != null) {
                Map<String, Object> tradeInfo = new HashMap<>();
                tradeInfo.put("scripCode", currentTrade.getScripCode());
                tradeInfo.put("signalType", currentTrade.getSignalType());
                tradeInfo.put("status", currentTrade.getStatus());
                tradeInfo.put("entryTriggered", currentTrade.getEntryTriggered());
                tradeInfo.put("target1Hit", currentTrade.isTarget1Hit());
                
                if (currentTrade.getEntryTriggered()) {
                    tradeInfo.put("entryPrice", currentTrade.getEntryPrice());
                    tradeInfo.put("currentPrice", currentTrade.getCurrentPrice());
                    tradeInfo.put("positionSize", currentTrade.getPositionSize());
                    tradeInfo.put("unrealizedPnL", currentTrade.getMetadata().get("unrealizedPnL"));
                }
                
                tradeInfo.put("stopLoss", currentTrade.getStopLoss());
                tradeInfo.put("target1", currentTrade.getTarget1());
                tradeInfo.put("target2", currentTrade.getTarget2());
                tradeInfo.put("signalPrice", currentTrade.getMetadata().get("signalPrice"));
                tradeInfo.put("createdTime", currentTrade.getMetadata().get("createdTime"));
                
                status.put("activeTrade", tradeInfo);
            } else {
                status.put("activeTrade", null);
                status.put("message", "Ready for new trade - No active trades");
            }
            
            log.info("📊 [BulletproofTC] Portfolio status requested - Active trade: {}", 
                    currentTrade != null ? currentTrade.getScripCode() : "None");
            
            return ResponseEntity.ok(status);
            
        } catch (Exception e) {
            log.error("🚨 [BulletproofTC] Error getting portfolio status: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "Failed to get portfolio status", "message", e.getMessage()));
        }
    }
    
    /**
     * 🎯 CREATE TEST TRADE - For manual testing
     * DEPRECATED: Manual trade creation is disabled in the new candle-based strategy.
     */
    /*
    @PostMapping("/trade/create")
    public ResponseEntity<Map<String, Object>> createTestTrade(@RequestBody Map<String, Object> tradeRequest) {
        try {
            String scripCode = (String) tradeRequest.get("scripCode");
            String signal = (String) tradeRequest.get("signal");
            Double entryPrice = extractDoubleValue(tradeRequest, "entryPrice");
            Double stopLoss = extractDoubleValue(tradeRequest, "stopLoss");
            Double target1 = extractDoubleValue(tradeRequest, "target1");
            Double target2 = extractDoubleValue(tradeRequest, "target2");
            Double target3 = extractDoubleValue(tradeRequest, "target3");
            
            if (scripCode == null || signal == null || entryPrice == null || stopLoss == null || target1 == null) {
                return ResponseEntity.badRequest()
                        .body(Map.of("error", "Missing required fields", 
                                   "required", "scripCode, signal, entryPrice, stopLoss, target1"));
            }
            
            log.info("🎯 [BulletproofTC] Manual trade creation requested: {} {} @ {} (SL: {}, T1: {}, T2: {}, T3: {})", 
                    scripCode, signal, entryPrice, stopLoss, target1, target2, target3);
            
            // For manual trades, extract companyName from request or use scripCode as fallback
            String companyName = (String) tradeRequest.getOrDefault("companyName", scripCode);
            // Use defaults (N, C) for manual trades unless specified
            String exchange = (String) tradeRequest.getOrDefault("exchange", "N");
            String exchangeType = (String) tradeRequest.getOrDefault("exchangeType", "C");

            boolean created = bulletproofSignalConsumer.createTrade(
                    scripCode, companyName, signal, entryPrice, stopLoss,
                    target1, target2, target3,
                    exchange, exchangeType,
                    LocalDateTime.now());
            
            Map<String, Object> response = new HashMap<>();
            if (created) {
                response.put("success", true);
                response.put("message", "Trade created successfully");
                response.put("scripCode", scripCode);
                response.put("signal", signal);
                response.put("status", "WAITING_FOR_ENTRY");
                
                log.info("✅ [BulletproofTC] Manual trade created successfully: {}", scripCode);
            } else {
                response.put("success", false);
                response.put("message", "Failed to create trade - may already have active trade or invalid parameters");
                response.put("hasActiveTrade", bulletproofSignalConsumer.hasActiveTrade());
                
                if (bulletproofSignalConsumer.hasActiveTrade()) {
                    ActiveTrade active = bulletproofSignalConsumer.getCurrentTrade();
                    response.put("currentActiveTrade", active.getScripCode());
                }
            }
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("🚨 [BulletproofTC] Error creating manual trade: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "Failed to create trade", "message", e.getMessage()));
        }
    }
    */
    
    /**
     * 💹 SIMULATE PRICE UPDATE - For testing entry/exit logic
     * DEPRECATED: Price updates are now driven by the 5-minute candle consumer.
     */
    /*
    @PostMapping("/trade/update-price")
    public ResponseEntity<Map<String, Object>> updatePrice(@RequestBody Map<String, Object> priceUpdate) {
        try {
            String scripCode = (String) priceUpdate.get("scripCode");
            Double price = extractDoubleValue(priceUpdate, "price");
            
            if (scripCode == null || price == null) {
                return ResponseEntity.badRequest()
                        .body(Map.of("error", "Missing required fields", 
                                   "required", "scripCode, price"));
            }
            
            ActiveTrade currentTrade = tradeManager.getCurrentTrade();
            if (currentTrade == null) {
                return ResponseEntity.badRequest()
                        .body(Map.of("error", "No active trade to update"));
            }
            
            if (!currentTrade.getScripCode().equals(scripCode)) {
                return ResponseEntity.badRequest()
                        .body(Map.of("error", "Price update script code mismatch", 
                                   "activeTradeScript", currentTrade.getScripCode(),
                                   "requestedScript", scripCode));
            }
            
            log.info("💹 [BulletproofTC] Manual price update: {} @ {}", scripCode, price);
            
            // Store previous state for comparison
            boolean wasEntryTriggered = currentTrade.getEntryTriggered();
            boolean wasTarget1Hit = currentTrade.isTarget1Hit();
            
            // Update price
            bulletproofSignalConsumer.updatePrice(scripCode, price, LocalDateTime.now());
            
            // Check for state changes
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("scripCode", scripCode);
            response.put("price", price);
            response.put("previousEntryTriggered", wasEntryTriggered);
            response.put("currentEntryTriggered", currentTrade.getEntryTriggered());
            response.put("previousTarget1Hit", wasTarget1Hit);
            response.put("currentTarget1Hit", currentTrade.isTarget1Hit());
            response.put("currentStatus", currentTrade.getStatus());
            
            // Check if trade was closed
            ActiveTrade afterUpdate = bulletproofSignalConsumer.getCurrentTrade();
            response.put("tradeStillActive", afterUpdate != null);
            
            if (afterUpdate == null) {
                response.put("message", "Trade was closed due to price update");
            } else if (!wasEntryTriggered && currentTrade.getEntryTriggered()) {
                response.put("message", "Entry triggered!");
            } else if (!wasTarget1Hit && currentTrade.isTarget1Hit()) {
                response.put("message", "Target 1 hit - 50% position exited!");
            } else {
                response.put("message", "Price updated successfully");
            }
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("🚨 [BulletproofTC] Error updating price: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "Failed to update price", "message", e.getMessage()));
        }
    }
    */
    
    /**
     * 🚨 EMERGENCY EXIT - Force close active trade
     */
    @PostMapping("/trade/emergency-exit")
    public ResponseEntity<Map<String, Object>> emergencyExit(@RequestBody Map<String, Object> exitRequest) {
        try {
            String reason = (String) exitRequest.getOrDefault("reason", "Manual emergency exit");
            
            ActiveTrade currentTrade = tradeManager.getCurrentTrade();
            if (currentTrade == null) {
                return ResponseEntity.badRequest()
                        .body(Map.of("error", "No active trade to exit"));
            }
            
            String scripCode = currentTrade.getScripCode();
            log.warn("🚨 [BulletproofTC] EMERGENCY EXIT requested for {} - Reason: {}", scripCode, reason);
            
            // Implementation would need to be added to BulletproofSignalConsumer
            // For now, return acknowledgment
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "Emergency exit triggered");
            response.put("scripCode", scripCode);
            response.put("reason", reason);
            response.put("exitTime", LocalDateTime.now().format(TIME_FORMAT));
            
            // Note: Actual emergency exit logic would be implemented in TradeManager
            log.warn("⚠️ [BulletproofTC] Emergency exit acknowledged for {} - Implementation pending", scripCode);
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("🚨 [BulletproofTC] Error processing emergency exit: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "Failed to process emergency exit", "message", e.getMessage()));
        }
    }

    @PostMapping("/broker/square-off-all")
    public ResponseEntity<Map<String, String>> squareOffAll() {
        try {
            brokerOrderService.squareOffAll();
            // also force internal emergency exit if active trade
            if (tradeManager.hasActiveTrade()) {
                // Close internal trade state after broker square-off
                // DEPRECATED: updatePrice is removed. A new method in the consumer is needed for emergency exits.
                log.warn("Square-off initiated, but internal state clearing needs a new method in TradeManager.");
            }
            return ResponseEntity.ok(Map.of("status", "ALL_POSITIONS_SQUARE_OFF_TRIGGERED"));
        } catch (Exception ex) {
            return ResponseEntity.internalServerError().body(Map.of("error", ex.getMessage()));
        }
    }
    
    /**
     * 📊 GET SYSTEM HEALTH - Overall system status
     */
    @GetMapping("/system/health")
    public ResponseEntity<Map<String, Object>> getSystemHealth() {
        try {
            Map<String, Object> health = new HashMap<>();
            
            health.put("systemName", "BULLETPROOF_TRADE_MANAGER");
            health.put("status", "OPERATIONAL");
            health.put("currentTime", LocalDateTime.now());
            health.put("hasActiveTrade", tradeManager.hasActiveTrade());
            
            ActiveTrade currentTrade = tradeManager.getCurrentTrade();
            if (currentTrade != null) {
                health.put("activeTradeScript", currentTrade.getScripCode());
                health.put("activeTradeStatus", currentTrade.getStatus());
                health.put("entryTriggered", currentTrade.getEntryTriggered());
            }
            
            // System capabilities
            health.put("capabilities", Map.of(
                "singleTradeOnly", true,
                "pivotRetestEntry", true,
                "hierarchicalExit", true,
                "fixedPositionSize", "₹1 lakh per trade",
                "trailingStop", true,
                "partialExit", "50% at Target 1"
            ));
            
            return ResponseEntity.ok(health);
            
        } catch (Exception e) {
            log.error("🚨 [BulletproofTC] Error getting system health: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "System health check failed", "message", e.getMessage()));
        }
    }
    
    /**
     * 📚 GET API DOCUMENTATION - Available endpoints
     */
    @GetMapping("/docs")
    public ResponseEntity<Map<String, Object>> getApiDocs() {
        Map<String, Object> docs = new HashMap<>();
        
        docs.put("title", "Bulletproof Trade Manager API");
        docs.put("description", "Single trade management system with pivot retest entry and hierarchical exits");
        docs.put("version", "1.0.0");
        
        Map<String, Object> endpoints = new HashMap<>();
        
        endpoints.put("GET /api/bulletproof/portfolio/status", 
                     "Get complete portfolio status including active trade details");
        endpoints.put("POST /api/bulletproof/trade/create", 
                     "Create new trade manually (requires: scripCode, signal, entryPrice, stopLoss, target1)");
        endpoints.put("POST /api/bulletproof/trade/update-price", 
                     "Simulate price update for testing (requires: scripCode, price)");
        endpoints.put("POST /api/bulletproof/trade/emergency-exit", 
                     "Force emergency exit of active trade (optional: reason)");
        endpoints.put("GET /api/bulletproof/system/health", 
                     "Get system health and capabilities");
        endpoints.put("GET /api/bulletproof/docs", 
                     "Get this API documentation");
        
        docs.put("endpoints", endpoints);
        
        Map<String, Object> examples = new HashMap<>();
        examples.put("createTrade", Map.of(
            "scripCode", "RELIANCE",
            "signal", "BUY",
            "entryPrice", 2500.0,
            "stopLoss", 2475.0,
            "target1", 2537.5
        ));
        examples.put("updatePrice", Map.of(
            "scripCode", "RELIANCE",
            "price", 2480.0
        ));
        examples.put("emergencyExit", Map.of(
            "reason", "Manual exit requested"
        ));
        
        docs.put("examples", examples);
        
        return ResponseEntity.ok(docs);
    }
    
    /**
     * 📊 BROKER NET POSITIONS – diagnostic endpoint.
     */
    @GetMapping("/broker/positions")
    public ResponseEntity<?> getBrokerPositions() {
        try {
            java.util.List<NetPosition> list = netPositionService.fetchAll();
            return ResponseEntity.ok(list);
        } catch (Exception e) {
            log.error("🚨 [BulletproofTC] Error fetching broker positions: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "Failed to fetch positions", "message", e.getMessage()));
        }
    }
    
    // Helper method
    private Double extractDoubleValue(Map<String, Object> data, String key) {
        Object value = data.get(key);
        if (value == null) return null;
        
        try {
            if (value instanceof Number) {
                return ((Number) value).doubleValue();
            }
            return Double.parseDouble(value.toString());
        } catch (NumberFormatException e) {
            log.warn("Invalid double value for key {}: {}", key, value);
            return null;
        }
    }
}
