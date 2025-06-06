package com.kotsin.execution.controller;

import com.kotsin.execution.service.TradeExecutionService;
import com.kotsin.execution.service.TradeStateManager;
import com.kotsin.execution.service.TradeHistoryService;
import com.kotsin.execution.service.TradingHoursService;
import com.kotsin.execution.model.ActiveTrade;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.HashMap;

/**
 * Test controller for validating trade execution APIs
 */
@RestController
@RequestMapping("/api/v1/test")
@RequiredArgsConstructor
@Slf4j
@CrossOrigin(origins = "*")
@Tag(name = "Testing & Debug", description = "APIs for testing system functionality, debugging, and manual signal execution")
public class TestController {
    
    private final TradeExecutionService tradeExecutionService;
    private final TradeStateManager tradeStateManager;
    private final TradeHistoryService tradeHistoryService;
    private final TradingHoursService tradingHoursService;
    
    /**
     * Test endpoint to manually create a trade
     * POST /api/v1/test/create-trade
     */
    @PostMapping("/create-trade")
    @Operation(summary = "Create a test trade", description = "Manually create a test trade")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Trade created successfully", content = @Content(schema = @Schema(implementation = Map.class))),
            @ApiResponse(responseCode = "500", description = "Failed to create test trade", content = @Content(schema = @Schema(implementation = Map.class)))
    })
    public ResponseEntity<Map<String, Object>> createTestTrade(
            @RequestParam String scripCode,
            @RequestParam String signal,
            @RequestParam Double entryPrice,
            @RequestParam Double stopLoss,
            @RequestParam Double target1) {
        
        try {
            log.info("🧪 Creating test trade: {} {} @ {}", signal, scripCode, entryPrice);
            
            tradeExecutionService.executeStrategySignal(
                    scripCode, 
                    signal, 
                    entryPrice, 
                    stopLoss, 
                    target1, 
                    "TEST_STRATEGY", 
                    "HIGH"
            );
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "Test trade created successfully");
            response.put("scripCode", scripCode);
            response.put("signal", signal);
            response.put("entryPrice", entryPrice);
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("🚨 Error creating test trade: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "Failed to create test trade", "message", e.getMessage()));
        }
    }
    
    /**
     * Get system status
     * GET /api/v1/test/status
     */
    @GetMapping("/status")
    @Operation(summary = "Get system status", description = "Retrieve the current status of the system")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "System status retrieved successfully", content = @Content(schema = @Schema(implementation = Map.class))),
            @ApiResponse(responseCode = "500", description = "Failed to get system status", content = @Content(schema = @Schema(implementation = Map.class)))
    })
    public ResponseEntity<Map<String, Object>> getSystemStatus() {
        try {
            Map<String, Object> status = new HashMap<>();
            status.put("status", "RUNNING");
            status.put("activeTrades", tradeStateManager.getActiveTradeCount());
            status.put("completedTrades", tradeHistoryService.getTradeCountStats());
            status.put("timestamp", java.time.LocalDateTime.now());
            
            return ResponseEntity.ok(status);
            
        } catch (Exception e) {
            log.error("🚨 Error getting system status: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "Failed to get system status"));
        }
    }
    
    /**
     * Check trading hours status
     * GET /api/v1/test/trading-hours
     */
    @GetMapping("/trading-hours")
    @Operation(summary = "Get trading hours status", description = "Retrieve the current trading hours status")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Trading hours status retrieved successfully", content = @Content(schema = @Schema(implementation = Map.class))),
            @ApiResponse(responseCode = "500", description = "Failed to get trading hours status", content = @Content(schema = @Schema(implementation = Map.class)))
    })
    public ResponseEntity<Map<String, Object>> getTradingHoursStatus() {
        try {
            Map<String, Object> status = new HashMap<>();
            
            java.time.LocalDateTime currentIST = tradingHoursService.getCurrentISTTime();
            status.put("currentIST", currentIST.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            status.put("currentISTTime", currentIST.format(DateTimeFormatter.ofPattern("HH:mm:ss")));
            
            status.put("nseStatus", tradingHoursService.isWithinTradingHours("NSE") ? "OPEN 🟢" : "CLOSED 🔴");
            status.put("mcxStatus", tradingHoursService.isWithinTradingHours("MCX") ? "OPEN 🟢" : "CLOSED 🔴");
            status.put("isWeekend", tradingHoursService.isWeekend());
            
            status.put("nseTimeUntilOpen", tradingHoursService.getTimeUntilMarketOpen("NSE"));
            status.put("mcxTimeUntilOpen", tradingHoursService.getTimeUntilMarketOpen("MCX"));
            
            // Test processing flags
            status.put("shouldProcessNSE", tradingHoursService.shouldProcessTrade("NSE", currentIST));
            status.put("shouldProcessMCX", tradingHoursService.shouldProcessTrade("MCX", currentIST));
            
            log.info("🕐 Trading hours status requested at {}", currentIST);
            
            return ResponseEntity.ok(status);
            
        } catch (Exception e) {
            log.error("🚨 Error getting trading hours status: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "Failed to get trading hours status"));
        }
    }
    
    /**
     * Get detailed active trades information
     * GET /api/v1/test/active-trades-debug
     */
    @GetMapping("/active-trades-debug")
    @Operation(summary = "Get active trades debug information", description = "Retrieve detailed information about active trades")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Active trades debug information retrieved successfully", content = @Content(schema = @Schema(implementation = Map.class))),
            @ApiResponse(responseCode = "500", description = "Failed to get active trades debug", content = @Content(schema = @Schema(implementation = Map.class)))
    })
    public ResponseEntity<Map<String, Object>> getActiveTradesDebug() {
        try {
            Map<String, ActiveTrade> activeTrades = tradeStateManager.getAllActiveTrades();
            Map<String, Object> response = new HashMap<>();
            
            response.put("totalActiveTrades", activeTrades.size());
            response.put("trades", activeTrades.values());
            response.put("timestamp", java.time.LocalDateTime.now());
            
            // Add summary by script and strategy
            Map<String, Integer> byScript = tradeStateManager.getTradeCountByScript();
            Map<String, Integer> byStrategy = tradeStateManager.getTradeCountByStrategy();
            
            response.put("tradesByScript", byScript);
            response.put("tradesByStrategy", byStrategy);
            
            // Log the request
            log.info("🔍 Active trades debug requested - Found {} trades", activeTrades.size());
            activeTrades.values().forEach(trade -> {
                log.info("📊 Active Trade: {} - {} {} - Status: {}, Entry: {}", 
                        trade.getTradeId(), trade.getSignalType(), trade.getScripCode(),
                        trade.getStatus(), trade.getEntryTriggered() ? trade.getEntryPrice() : "Waiting");
            });
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("🚨 Error getting active trades debug: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "Failed to get active trades debug"));
        }
    }
    
    /**
     * Test market data simulation
     * POST /api/v1/test/simulate-price
     */
    @PostMapping("/simulate-price")
    @Operation(summary = "Test market data simulation", description = "Simulate a price update for a given scrip code")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Price simulation executed successfully", content = @Content(schema = @Schema(implementation = Map.class))),
            @ApiResponse(responseCode = "500", description = "Failed to simulate price", content = @Content(schema = @Schema(implementation = Map.class)))
    })
    public ResponseEntity<Map<String, Object>> simulatePrice(
            @RequestParam String scripCode,
            @RequestParam Double price) {
        
        try {
            log.info("🧪 Simulating price update: {} @ {}", scripCode, price);
            
            // Simulate a price update
            tradeExecutionService.updateTradeWithPrice(scripCode, price, java.time.LocalDateTime.now());
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "Price simulation executed");
            response.put("scripCode", scripCode);
            response.put("price", price);
            response.put("timestamp", java.time.LocalDateTime.now());
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("🚨 Error simulating price: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "Failed to simulate price", "message", e.getMessage()));
        }
    }
    
    /**
     * Test signal logging
     * POST /api/v1/test/log-signal
     */
    @PostMapping("/log-signal")
    @Operation(summary = "Test signal logging", description = "Log a test signal")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Signal logged successfully", content = @Content(schema = @Schema(implementation = Map.class))),
            @ApiResponse(responseCode = "500", description = "Failed to log signal", content = @Content(schema = @Schema(implementation = Map.class)))
    })
    public ResponseEntity<Map<String, Object>> logTestSignal(
            @RequestParam String scripCode,
            @RequestParam String signal,
            @RequestParam String strategy,
            @RequestParam(defaultValue = "test") String reason,
            @RequestParam(defaultValue = "true") boolean executed) {
        
        try {
            tradeHistoryService.logSignal(scripCode, signal, strategy, reason, executed);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "Signal logged successfully");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("🚨 Error logging test signal: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "Failed to log signal"));
        }
    }
    
    /**
     * Test timezone conversion
     * GET /api/v1/test/timezone-debug
     */
    @GetMapping("/timezone-debug")
    @Operation(summary = "Test timezone conversion", description = "Convert UTC time to IST and check trading hours")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Timezone conversion information retrieved successfully", content = @Content(schema = @Schema(implementation = Map.class))),
            @ApiResponse(responseCode = "500", description = "Failed to get timezone debug info", content = @Content(schema = @Schema(implementation = Map.class)))
    })
    public ResponseEntity<Map<String, Object>> getTimezoneDebug(
            @RequestParam(required = false) String utcTime) {
        
        try {
            Map<String, Object> response = new HashMap<>();
            
            // Current times
            java.time.LocalDateTime currentUTC = java.time.LocalDateTime.now(java.time.ZoneId.of("UTC"));
            java.time.LocalDateTime currentIST = java.time.LocalDateTime.now(java.time.ZoneId.of("Asia/Kolkata"));
            
            response.put("currentUTC", currentUTC.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            response.put("currentIST", currentIST.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            
            // Time difference
            long offsetMinutes = java.time.Duration.between(currentUTC, currentIST).toMinutes();
            response.put("offsetMinutes", offsetMinutes);
            response.put("offsetHours", offsetMinutes / 60.0);
            
            // If UTC time provided, convert it
            if (utcTime != null && !utcTime.isEmpty()) {
                try {
                    java.time.LocalDateTime parsedUTC = java.time.LocalDateTime.parse(utcTime, 
                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                    
                    // Convert to IST
                    java.time.ZonedDateTime utcZoned = parsedUTC.atZone(java.time.ZoneId.of("UTC"));
                    java.time.ZonedDateTime istZoned = utcZoned.withZoneSameInstant(java.time.ZoneId.of("Asia/Kolkata"));
                    java.time.LocalDateTime convertedIST = istZoned.toLocalDateTime();
                    
                    response.put("providedUTC", utcTime);
                    response.put("convertedIST", convertedIST.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                    
                    // Check if converted time is within trading hours
                    boolean withinNSE = tradingHoursService.isWithinTradingHours("NSE", convertedIST);
                    boolean shouldProcess = tradingHoursService.shouldProcessTrade("NSE", convertedIST);
                    
                    response.put("withinNSETradingHours", withinNSE);
                    response.put("shouldProcessTrade", shouldProcess);
                    
                } catch (Exception e) {
                    response.put("conversionError", "Failed to parse UTC time: " + e.getMessage());
                }
            }
            
            // Example conversions
            Map<String, String> examples = new HashMap<>();
            examples.put("07:58 UTC", "13:28 IST (1:28 PM)");
            examples.put("03:45 UTC", "09:15 IST (Market Open)");
            examples.put("10:00 UTC", "15:30 IST (Market Close)");
            examples.put("12:00 UTC", "17:30 IST (After Market)");
            
            response.put("conversionExamples", examples);
            
            log.info("🕐 Timezone debug requested - UTC: {}, IST: {}, Offset: {} hours", 
                    currentUTC, currentIST, offsetMinutes / 60.0);
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("🚨 Error in timezone debug: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "Failed to get timezone debug info"));
        }
    }
    
    /**
     * Debug market data and script matching
     * GET /api/v1/test/market-data-debug
     */
    @GetMapping("/market-data-debug")
    @Operation(summary = "Debug market data and script matching", description = "Check what script codes are in market data vs active trades")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Market data debug info retrieved successfully"),
            @ApiResponse(responseCode = "500", description = "Failed to get market data debug info")
    })
    public ResponseEntity<Map<String, Object>> getMarketDataDebug() {
        try {
            Map<String, Object> response = new HashMap<>();
            
            // Get active trades
            Map<String, ActiveTrade> activeTrades = tradeStateManager.getAllActiveTrades();
            Map<String, String> activeTradeInfo = new HashMap<>();
            
            activeTrades.values().forEach(trade -> {
                activeTradeInfo.put(trade.getScripCode(), 
                    String.format("%s - %s - %s", 
                        trade.getTradeId().substring(0, Math.min(20, trade.getTradeId().length())), 
                        trade.getSignalType(), 
                        trade.getStatus()));
            });
            
            response.put("activeTrades", activeTrades.size());
            response.put("activeTradeDetails", activeTradeInfo);
            response.put("activeScriptCodes", activeTrades.values().stream()
                    .map(ActiveTrade::getScripCode)
                    .distinct()
                    .sorted()
                    .toList());
            
            // Add debugging info about what we expect
            response.put("expectedScripts", "Market data should contain: 134082, 105379, 10243, 145997");
            response.put("marketDataProcessing", "Check LiveMarketDataConsumer logs for script code extraction");
            
            // Last update info
            response.put("timestamp", java.time.LocalDateTime.now());
            response.put("instructions", "Market data ticks should have 'companyName' field matching our script codes");
            
            log.info("🔍 Market data debug requested - {} active trades for scripts: {}", 
                    activeTrades.size(), 
                    activeTrades.values().stream().map(ActiveTrade::getScripCode).distinct().toList());
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("🚨 Error in market data debug: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "Failed to get market data debug info"));
        }
    }
} 