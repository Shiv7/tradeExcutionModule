# 🔧 CRITICAL FIXES IMPLEMENTATION GUIDE - TOP 20 ISSUES

## ✅ **FIX #1: Add Comprehensive Test Suite**

### **Status:** ✅ IMPLEMENTED (StrategySignalTest.java created)

### **Files Created:**
- `src/test/java/com/kotsin/execution/model/StrategySignalTest.java`
- `src/main/java/com/kotsin/execution/validation/ValidationResult.java`
- `src/main/java/com/kotsin/execution/validation/SignalValidator.java`

### **Next Steps:**
Add to `pom.xml`:
```xml
<dependencies>
    <!-- JUnit 5 -->
    <dependency>
        <groupId>org.junit.jupiter</groupId>
        <artifactId>junit-jupiter</artifactId>
        <version>5.10.1</version>
        <scope>test</scope>
    </dependency>

    <!-- Mockito -->
    <dependency>
        <groupId>org.mockito</groupId>
        <artifactId>mockito-core</artifactId>
        <version>5.8.0</version>
        <scope>test</scope>
    </dependency>

    <!-- Spring Boot Test -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-test</artifactId>
        <scope>test</scope>
    </dependency>
</dependencies>
```

Run tests:
```bash
mvn test
```

---

## 🔥 **FIX #2: Implement BigDecimal for Financial Calculations**

### **Problem:**
123+ double operations cause floating-point precision errors.

### **Solution:**

#### **Create FinancialMath utility:**

```java
package com.kotsin.execution.util;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * 🛡️ CRITICAL FIX #2: Financial calculations with BigDecimal
 * Eliminates floating-point precision errors
 */
public class FinancialMath {

    private static final int PRICE_SCALE = 2;  // 2 decimal places for prices
    private static final int PERCENTAGE_SCALE = 4;  // 4 decimal places for %
    private static final RoundingMode ROUNDING = RoundingMode.HALF_EVEN;  // Banker's rounding

    /**
     * Calculate profit/loss for a position
     */
    public static BigDecimal calculatePnL(BigDecimal entryPrice, BigDecimal exitPrice,
                                          int quantity, boolean isLong) {
        BigDecimal priceDiff = isLong
            ? exitPrice.subtract(entryPrice)
            : entryPrice.subtract(exitPrice);

        return priceDiff.multiply(BigDecimal.valueOf(quantity))
                        .setScale(PRICE_SCALE, ROUNDING);
    }

    /**
     * Calculate risk-reward ratio
     */
    public static BigDecimal calculateRiskRewardRatio(BigDecimal entryPrice,
                                                       BigDecimal stopLoss,
                                                       BigDecimal target) {
        BigDecimal risk = entryPrice.subtract(stopLoss).abs();
        BigDecimal reward = target.subtract(entryPrice).abs();

        if (risk.compareTo(BigDecimal.ZERO) == 0) {
            throw new IllegalArgumentException("Risk cannot be zero");
        }

        return reward.divide(risk, PERCENTAGE_SCALE, ROUNDING);
    }

    /**
     * Calculate percentage change
     */
    public static BigDecimal calculatePercentageChange(BigDecimal oldValue,
                                                        BigDecimal newValue) {
        if (oldValue.compareTo(BigDecimal.ZERO) == 0) {
            throw new IllegalArgumentException("Old value cannot be zero");
        }

        return newValue.subtract(oldValue)
                       .divide(oldValue, PERCENTAGE_SCALE, ROUNDING)
                       .multiply(BigDecimal.valueOf(100));
    }

    /**
     * Round price to tick size
     */
    public static BigDecimal roundToTickSize(BigDecimal price, BigDecimal tickSize) {
        if (tickSize.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Tick size must be positive");
        }

        return price.divide(tickSize, 0, ROUNDING)
                    .multiply(tickSize)
                    .setScale(PRICE_SCALE, ROUNDING);
    }

    /**
     * Calculate position size based on risk percentage
     */
    public static int calculatePositionSize(BigDecimal accountCapital,
                                           BigDecimal riskPercentage,
                                           BigDecimal entryPrice,
                                           BigDecimal stopLoss) {
        BigDecimal riskAmount = accountCapital
            .multiply(riskPercentage)
            .divide(BigDecimal.valueOf(100), PRICE_SCALE, ROUNDING);

        BigDecimal riskPerShare = entryPrice.subtract(stopLoss).abs();

        if (riskPerShare.compareTo(BigDecimal.ZERO) == 0) {
            return 0;
        }

        return riskAmount.divide(riskPerShare, 0, RoundingMode.DOWN).intValue();
    }

    /**
     * Convert double to BigDecimal safely
     */
    public static BigDecimal toBigDecimal(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            throw new IllegalArgumentException("Cannot convert NaN or Infinity to BigDecimal");
        }
        return BigDecimal.valueOf(value);
    }
}
```

#### **Test Cases:**

```java
package com.kotsin.execution.util;

import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import static org.junit.jupiter.api.Assertions.*;

class FinancialMathTest {

    @Test
    void testPnLCalculationLong() {
        BigDecimal entry = new BigDecimal("4120.50");
        BigDecimal exit = new BigDecimal("4150.75");
        int qty = 100;

        BigDecimal pnl = FinancialMath.calculatePnL(entry, exit, qty, true);

        assertEquals(new BigDecimal("3025.00"), pnl);
    }

    @Test
    void testRiskRewardRatio() {
        BigDecimal entry = new BigDecimal("4120.00");
        BigDecimal sl = new BigDecimal("4100.00");
        BigDecimal target = new BigDecimal("4150.00");

        BigDecimal rr = FinancialMath.calculateRiskRewardRatio(entry, sl, target);

        assertEquals(new BigDecimal("1.5000"), rr);
    }

    @Test
    void testFloatingPointPrecision() {
        // Classic floating point problem: 0.1 + 0.2 != 0.3
        BigDecimal a = new BigDecimal("0.1");
        BigDecimal b = new BigDecimal("0.2");
        BigDecimal sum = a.add(b);

        assertEquals(new BigDecimal("0.3"), sum);  // ✅ Works with BigDecimal

        // But with double:
        double d1 = 0.1;
        double d2 = 0.2;
        double dSum = d1 + d2;
        assertNotEquals(0.3, dSum);  // ❌ Fails with double!
        assertEquals(0.30000000000000004, dSum);  // Actual double result
    }

    @Test
    void testTickSizeRounding() {
        BigDecimal price = new BigDecimal("4123.47");
        BigDecimal tickSize = new BigDecimal("0.05");  // NIFTY tick size

        BigDecimal rounded = FinancialMath.roundToTickSize(price, tickSize);

        assertEquals(new BigDecimal("4123.45"), rounded);
    }
}
```

#### **Migration Strategy:**

1. **Phase 1:** Introduce FinancialMath alongside existing double calculations
2. **Phase 2:** Update BacktestTrade.calculatePnL() to use BigDecimal
3. **Phase 3:** Update TradeManager exit calculations
4. **Phase 4:** Deprecate double-based methods

---

## 🚨 **FIX #3: Fix waitingTrades.clear() Nuclear Bug**

### **Problem:**
`TradeManager.java:126` - Executing one trade deletes ALL waiting trades.

### **Current Code:**
```java
if (bestTrade != null) {
    executeEntry(bestTrade, candle);
    activeTrade.set(bestTrade);
    waitingTrades.clear();  // 💣 DELETES EVERYTHING!
}
```

### **Fixed Code:**

```java
if (bestTrade != null) {
    log.info("Executing best trade: {} with R:R={}",
             bestTrade.getScripCode(),
             bestTrade.getMetadata().getOrDefault("potentialRR", 0.0));

    executeEntry(bestTrade, candle);
    activeTrade.set(bestTrade);

    // 🛡️ CRITICAL FIX #3: Only remove the executed trade, keep others
    waitingTrades.remove(bestTrade.getScripCode());

    log.info("Trade {} activated. Remaining waiting trades: {}",
             bestTrade.getScripCode(), waitingTrades.size());

    // Log what trades are still waiting
    if (!waitingTrades.isEmpty()) {
        waitingTrades.values().forEach(t ->
            log.info("Still waiting: {} with R:R={}",
                     t.getScripCode(),
                     t.getMetadata().getOrDefault("potentialRR", 0.0))
        );
    }
}
```

### **Additional Enhancement - Max Active Trades:**

```java
// At start of processCandle method
if (activeTrade.get() != null) {
    evaluateAndMaybeExit(activeTrade.get(), candle);
    return;
}

// 🛡️ NEW: Check max active trades from config
int maxActiveTrades = tradeProps.getMaxActiveTrades();  // Default: 1
if (waitingTrades.size() >= maxActiveTrades * 5) {  // 5x buffer
    log.warn("Too many waiting trades ({}), purging old ones", waitingTrades.size());
    purgeOldWaitingTrades();
}
```

### **Test Case:**

```java
@Test
void testWaitingTradesNotClearedOnExecution() {
    // Setup: 3 waiting trades
    ActiveTrade trade1 = createTrade("49812", 3.0);
    ActiveTrade trade2 = createTrade("49813", 2.5);
    ActiveTrade trade3 = createTrade("49814", 2.8);

    tradeManager.addSignalToWatchlist(signal1, LocalDateTime.now());
    tradeManager.addSignalToWatchlist(signal2, LocalDateTime.now());
    tradeManager.addSignalToWatchlist(signal3, LocalDateTime.now());

    assertEquals(3, tradeManager.getWaitingTrades().size());

    // Trigger execution of best trade (trade1 with R:R=3.0)
    Candlestick candle = createValidCandleForEntry(trade1);
    tradeManager.processCandle(candle);

    // 🛡️ FIX VERIFICATION: Other trades should still be waiting
    assertEquals(2, tradeManager.getWaitingTrades().size());
    assertTrue(tradeManager.getWaitingTrades().containsKey("49813"));
    assertTrue(tradeManager.getWaitingTrades().containsKey("49814"));
    assertFalse(tradeManager.getWaitingTrades().containsKey("49812"));
}
```

---

## ⚡ **FIX #4: Implement Dynamic Position Sizing**

### **Problem:**
`TradeManager.java:51` - POSITION_SIZE hardcoded to 1, ignores signal multiplier.

### **Solution:**

```java
package com.kotsin.execution.service;

import com.kotsin.execution.config.TradeProps;
import com.kotsin.execution.model.ActiveTrade;
import com.kotsin.execution.model.StrategySignal;
import com.kotsin.execution.util.FinancialMath;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

/**
 * 🛡️ CRITICAL FIX #59: Dynamic position sizing based on risk
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class DynamicPositionSizer {

    private final TradeProps tradeProps;
    private final PortfolioRiskManager riskManager;

    /**
     * Calculate position size based on account capital, risk %, and signal confidence
     */
    public int calculatePositionSize(StrategySignal signal, BigDecimal accountCapital) {
        // 1. Base risk percentage from config
        double baseRiskPercent = tradeProps.getDefaultRiskPercentage();  // e.g., 1.5%

        // 2. Adjust by signal confidence (0.5 - 1.5x multiplier)
        double confidenceMultiplier = 0.5 + (signal.getConfidence() * 1.0);

        // 3. Apply position size multiplier from signal
        double signalMultiplier = signal.getPositionSizeMultiplier();
        if (signalMultiplier <= 0) {
            signalMultiplier = 1.0;  // Default
        }

        // 4. Final risk percentage
        double finalRiskPercent = baseRiskPercent * confidenceMultiplier * signalMultiplier;

        // 5. Cap at max risk per trade
        double maxRiskPercent = tradeProps.getMaxRiskPerTradePercentage();  // e.g., 2.0%
        finalRiskPercent = Math.min(finalRiskPercent, maxRiskPercent);

        // 6. Calculate position size
        BigDecimal entry = BigDecimal.valueOf(signal.getEntryPrice());
        BigDecimal sl = BigDecimal.valueOf(signal.getStopLoss());

        int calculatedSize = FinancialMath.calculatePositionSize(
            accountCapital,
            BigDecimal.valueOf(finalRiskPercent),
            entry,
            sl
        );

        // 7. Apply max position size limit
        int maxSize = tradeProps.getMaxPositionSize();  // e.g., 5000
        calculatedSize = Math.min(calculatedSize, maxSize);

        // 8. Minimum 1 lot
        calculatedSize = Math.max(calculatedSize, 1);

        log.info("Position sizing: scrip={} baseRisk={}% confidence={} signalMult={} " +
                 "finalRisk={}% accountCapital={} calculatedSize={}",
                 signal.getScripCode(), baseRiskPercent, signal.getConfidence(),
                 signalMultiplier, finalRiskPercent, accountCapital, calculatedSize);

        return calculatedSize;
    }

    /**
     * Validate position size against portfolio constraints
     */
    public boolean validatePositionSize(ActiveTrade trade, int positionSize,
                                       BigDecimal accountCapital) {
        BigDecimal positionValue = BigDecimal.valueOf(trade.getEntryPrice())
                                             .multiply(BigDecimal.valueOf(positionSize));

        // Check against account capital
        double positionPercent = positionValue.divide(accountCapital, 4,
                                                     java.math.RoundingMode.HALF_EVEN)
                                             .multiply(BigDecimal.valueOf(100))
                                             .doubleValue();

        double maxSinglePosition = tradeProps.getMaxSinglePositionPercent();  // e.g., 10%

        if (positionPercent > maxSinglePosition) {
            log.warn("Position size too large: {}% of capital (max: {}%)",
                     positionPercent, maxSinglePosition);
            return false;
        }

        // Check against portfolio risk limits
        return riskManager.canAddPosition(trade, positionSize);
    }
}
```

### **Update TradeManager:**

```java
// Remove hardcoded constant
// private static final int POSITION_SIZE = 1;  // ❌ DELETE

// Inject new services
private final DynamicPositionSizer positionSizer;
private final AccountBalanceService accountBalanceService;

private void executeEntry(ActiveTrade trade, Candlestick confirmationCandle) {
    double entryPrice = confirmationCandle.getClose();

    // 🛡️ CRITICAL FIX #59: Dynamic position sizing
    BigDecimal accountCapital = accountBalanceService.getCurrentCapital();
    StrategySignal originalSignal = reconstructSignalFromTrade(trade);

    int positionSize = positionSizer.calculatePositionSize(originalSignal, accountCapital);

    // Validate position size
    if (!positionSizer.validatePositionSize(trade, positionSize, accountCapital)) {
        log.error("Position size validation failed for {}, aborting entry", trade.getScripCode());
        trade.setStatus(ActiveTrade.TradeStatus.CANCELLED);
        return;
    }

    trade.setPositionSize(positionSize);
    // ... rest of entry logic
}
```

---

## 🎯 **FIX #5: Fix Exit Priority Logic**

### **Problem:**
`TradeManager.java:409-417` - Cannot determine if SL or TP hit first on same candle.

### **Current Broken Code:**
```java
boolean hitSL = trade.isBullish() ? bar.getLow() <= trade.getStopLoss()
        : bar.getHigh() >= trade.getStopLoss();
boolean hitT1 = trade.isBullish() ? bar.getHigh() >= trade.getTarget1()
        : bar.getLow() <= trade.getTarget1();

if (!hitSL && !hitT1) return;

String reason = hitSL ? "STOP_LOSS" : "TARGET1";  // ❌ WRONG!
double exitPrice = hitSL ? trade.getStopLoss() : trade.getTarget1();
```

### **Fixed Code - OHLC Sequence Analysis:**

```java
/**
 * 🛡️ CRITICAL FIX #61: Determine which level hit first using OHLC sequence
 *
 * For LONG trades:
 * - If open < SL: Price gapped down → SL hit first
 * - If open > TP: Price gapped up → TP hit first
 * - Otherwise: Use high/low sequence
 *
 * Sequence rules:
 * 1. If close > open (bullish bar): low hit before high
 * 2. If close < open (bearish bar): high hit before low
 */
private ExitResult determineExitPriority(ActiveTrade trade, Candlestick bar) {
    double open = bar.getOpen();
    double high = bar.getHigh();
    double low = bar.getLow();
    double close = bar.getClose();
    double sl = trade.getStopLoss();
    double tp = trade.getTarget1();

    if (trade.isBullish()) {
        boolean slHit = low <= sl;
        boolean tpHit = high >= tp;

        if (!slHit && !tpHit) {
            return new ExitResult(false, 0, null);
        }

        if (slHit && !tpHit) {
            return new ExitResult(true, sl, "STOP_LOSS");
        }

        if (tpHit && !slHit) {
            return new ExitResult(true, tp, "TARGET1");
        }

        // Both hit - determine priority

        // Case 1: Gap down (open below SL)
        if (open <= sl) {
            log.info("Gap down detected: open={} <= SL={}, SL hit first", open, sl);
            return new ExitResult(true, sl, "STOP_LOSS");
        }

        // Case 2: Gap up (open above TP)
        if (open >= tp) {
            log.info("Gap up detected: open={} >= TP={}, TP hit first", open, tp);
            return new ExitResult(true, tp, "TARGET1");
        }

        // Case 3: Both within range - use sequence analysis
        boolean isBullishBar = close > open;

        if (isBullishBar) {
            // Bullish bar: low (SL) hit before high (TP)
            log.info("Bullish bar: low hit first, SL={} before TP={}", sl, tp);
            return new ExitResult(true, sl, "STOP_LOSS");
        } else {
            // Bearish bar: high (TP) hit before low (SL)
            log.info("Bearish bar: high hit first, TP={} before SL={}", tp, sl);
            return new ExitResult(true, tp, "TARGET1");
        }

    } else {
        // SHORT trade logic (inverse)
        boolean slHit = high >= sl;
        boolean tpHit = low <= tp;

        if (!slHit && !tpHit) {
            return new ExitResult(false, 0, null);
        }

        if (slHit && !tpHit) {
            return new ExitResult(true, sl, "STOP_LOSS");
        }

        if (tpHit && !slHit) {
            return new ExitResult(true, tp, "TARGET1");
        }

        // Both hit - determine priority for SHORT

        if (open >= sl) {
            // Gap up through SL
            return new ExitResult(true, sl, "STOP_LOSS");
        }

        if (open <= tp) {
            // Gap down through TP
            return new ExitResult(true, tp, "TARGET1");
        }

        boolean isBullishBar = close > open;

        if (isBullishBar) {
            // Bullish bar: low (TP) hit before high (SL)
            return new ExitResult(true, tp, "TARGET1");
        } else {
            // Bearish bar: high (SL) hit before low (TP)
            return new ExitResult(true, sl, "STOP_LOSS");
        }
    }
}

// Helper class
private record ExitResult(boolean shouldExit, double exitPrice, String reason) {}
```

### **Test Cases:**

```java
@Test
void testExitPriority_LongTrade_BullishBar_BothHit() {
    ActiveTrade trade = createLongTrade(100.0, 95.0, 110.0);  // Entry, SL, TP

    // Bullish bar: low hit first (SL), then high (TP)
    Candlestick bar = Candlestick.builder()
        .open(98.0)
        .low(94.0)   // Hits SL
        .high(112.0) // Hits TP
        .close(105.0)
        .build();

    ExitResult result = tradeManager.determineExitPriority(trade, bar);

    assertTrue(result.shouldExit());
    assertEquals("STOP_LOSS", result.reason());
    assertEquals(95.0, result.exitPrice());
}

@Test
void testExitPriority_LongTrade_BearishBar_BothHit() {
    ActiveTrade trade = createLongTrade(100.0, 95.0, 110.0);

    // Bearish bar: high hit first (TP), then low (SL)
    Candlestick bar = Candlestick.builder()
        .open(105.0)
        .high(112.0) // Hits TP first
        .low(94.0)   // Hits SL second
        .close(96.0)
        .build();

    ExitResult result = tradeManager.determineExitPriority(trade, bar);

    assertTrue(result.shouldExit());
    assertEquals("TARGET1", result.reason());
    assertEquals(110.0, result.exitPrice());
}

@Test
void testExitPriority_GapDown() {
    ActiveTrade trade = createLongTrade(100.0, 95.0, 110.0);

    // Gap down below SL
    Candlestick bar = Candlestick.builder()
        .open(92.0)  // Opens below SL!
        .low(90.0)
        .high(111.0) // Recovers to hit TP
        .close(108.0)
        .build();

    ExitResult result = tradeManager.determineExitPriority(trade, bar);

    assertEquals("STOP_LOSS", result.reason());  // Gap takes priority
}
```

---

## 📊 **FIX #6: Add Slippage Modeling to Backtest**

### **Problem:**
`BacktestEngine.java:174` - Assumes perfect fills at signal price.

### **Solution:**

```java
package com.kotsin.execution.service;

import com.kotsin.execution.model.Candlestick;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * 🛡️ CRITICAL FIX #66: Realistic slippage modeling for backtests
 */
@Service
@Slf4j
public class SlippageCalculator {

    @Value("${backtest.slippage.equity.bps:5}")
    private int equitySlippageBps;  // 5 basis points = 0.05%

    @Value("${backtest.slippage.options.bps:10}")
    private int optionsSlippageBps;  // 10 basis points = 0.10%

    @Value("${backtest.slippage.mcx.bps:8}")
    private int mcxSlippageBps;  // 8 basis points

    /**
     * Calculate realistic entry price with slippage
     */
    public double calculateEntryPriceWithSlippage(double signalPrice, String exchange,
                                                   String exchangeType, boolean isBullish,
                                                   Candlestick candle) {
        // Determine slippage based on instrument type
        int slippageBps = getSlippageBps(exchange, exchangeType);

        // For LONG: pay the ask (higher), for SHORT: receive the bid (lower)
        double slippagePercent = slippageBps / 10000.0;
        double slippage = signalPrice * slippagePercent;

        double entryWithSlippage = isBullish
            ? signalPrice + slippage  // Pay more for LONG
            : signalPrice - slippage; // Receive less for SHORT

        // Validate entry is within candle range
        if (entryWithSlippage < candle.getLow() || entryWithSlippage > candle.getHigh()) {
            log.warn("Slippage resulted in price outside candle range, adjusting: " +
                     "signal={}, withSlippage={}, candleLow={}, candleHigh={}",
                     signalPrice, entryWithSlippage, candle.getLow(), candle.getHigh());

            // Clamp to candle range
            entryWithSlippage = Math.max(candle.getLow(), Math.min(candle.getHigh(), entryWithSlippage));
        }

        return entryWithSlippage;
    }

    /**
     * Calculate realistic exit price with slippage
     */
    public double calculateExitPriceWithSlippage(double targetPrice, String exchange,
                                                  String exchangeType, boolean isBullish,
                                                  boolean isStopLoss) {
        int slippageBps = getSlippageBps(exchange, exchangeType);

        // Stop losses experience MORE slippage (market orders in volatile conditions)
        if (isStopLoss) {
            slippageBps = (int) (slippageBps * 1.5);  // 50% more slippage on stops
        }

        double slippagePercent = slippageBps / 10000.0;
        double slippage = targetPrice * slippagePercent;

        // For LONG exit: receive less, for SHORT exit: pay more
        return isBullish
            ? targetPrice - slippage  // Receive less when selling
            : targetPrice + slippage; // Pay more when covering short
    }

    /**
     * Check if order would have filled given bid/ask spread
     */
    public boolean wouldOrderFill(double limitPrice, Candlestick candle, boolean isBuy) {
        // Estimate bid/ask from candle
        double mid = (candle.getHigh() + candle.getLow()) / 2.0;
        double spread = (candle.getHigh() - candle.getLow()) * 0.1;  // Assume 10% of range

        double estimatedAsk = mid + (spread / 2.0);
        double estimatedBid = mid - (spread / 2.0);

        if (isBuy) {
            // Buy limit order fills if ask <= limit price
            boolean fills = estimatedAsk <= limitPrice;
            if (!fills) {
                log.debug("Buy limit order would not fill: limit={}, estimatedAsk={}",
                          limitPrice, estimatedAsk);
            }
            return fills;
        } else {
            // Sell limit order fills if bid >= limit price
            boolean fills = estimatedBid >= limitPrice;
            if (!fills) {
                log.debug("Sell limit order would not fill: limit={}, estimatedBid={}",
                          limitPrice, estimatedBid);
            }
            return fills;
        }
    }

    private int getSlippageBps(String exchange, String exchangeType) {
        if ("M".equalsIgnoreCase(exchange)) {
            return mcxSlippageBps;
        }
        if ("D".equalsIgnoreCase(exchangeType)) {
            return optionsSlippageBps;
        }
        return equitySlippageBps;
    }
}
```

### **Update BacktestEngine:**

```java
private final SlippageCalculator slippageCalculator;

private void executeEntry(BacktestTrade trade, Candlestick candle, LocalDateTime time) {
    trade.setEntryTime(time);

    // 🛡️ CRITICAL FIX #66: Apply realistic slippage
    double entryWithSlippage = slippageCalculator.calculateEntryPriceWithSlippage(
        trade.getSignalPrice(),
        trade.getExchange(),
        trade.getExchangeType(),
        trade.isBullish(),
        candle
    );

    trade.setEntryPrice(entryWithSlippage);
    trade.setStatus(BacktestTrade.TradeStatus.ACTIVE);

    log.info("Backtest entry with slippage: signal={}, actual={}, slippage={}",
             trade.getSignalPrice(), entryWithSlippage,
             Math.abs(entryWithSlippage - trade.getSignalPrice()));
}
```

---

## 🕐 **FIX #7: Fix Time-Travel Bug (Math.abs)**

### **Problem:**
`SignalConsumer.java:111` - Future timestamps treated as old signals.

### **Current Broken Code:**
```java
long ageSeconds = Math.abs(receivedAt.getEpochSecond() - signalTs.getEpochSecond());
```

### **Fixed Code:**

```java
// 🛡️ CRITICAL FIX #25: Remove Math.abs to detect future timestamps
long ageSeconds = receivedAt.getEpochSecond() - signalTs.getEpochSecond();

// Detect future timestamps (clock skew or bad data)
if (ageSeconds < 0) {
    log.error("signal_from_future scrip={} signalTime={} receivedTime={} skew={}sec - REJECTED",
              raw.getScripCode(), signalTimeIst, receivedIst, Math.abs(ageSeconds));
    ack.acknowledge();
    return;
}

// Continue with normal age-based routing
if (ageSeconds > BACKTEST_THRESHOLD_SECONDS) {
    // BACKTEST mode
} else {
    // LIVE mode
}
```

### **Test Case:**

```java
@Test
void testFutureSignalRejected() {
    long futureTime = System.currentTimeMillis() + 3600_000;  // 1 hour in future

    StrategySignal signal = StrategySignal.builder()
        .scripCode("N:D:49812")
        .timestamp(futureTime)
        .build();

    // Should be rejected, not routed to backtest
    signalConsumer.processStrategySignal(signal, ack, System.currentTimeMillis(), record);

    verify(backtestEngine, never()).runBacktest(any(), any());
    verify(tradeManager, never()).addSignalToWatchlist(any(), any());
    verify(ack, times(1)).acknowledge();
}
```

---

## 🔐 **FIX #8: Move Credentials to Vault**

### **Problem:**
`application.properties` - Plaintext credentials.

### **Solution - Use Spring Cloud Vault:**

#### **Add dependencies:**

```xml
<dependency>
    <groupId>org.springframework.cloud</groupId>
    <artifactId>spring-cloud-starter-vault-config</artifactId>
    <version>4.1.0</version>
</dependency>
```

#### **Create bootstrap.yml:**

```yaml
spring:
  application:
    name: trade-execution-module
  cloud:
    vault:
      uri: http://localhost:8200
      token: ${VAULT_TOKEN}  # From environment
      kv:
        enabled: true
        backend: secret
        profile-separator: /
      authentication: TOKEN
```

#### **Store credentials in Vault:**

```bash
# Login to Vault
vault login ${VAULT_TOKEN}

# Store 5Paisa credentials
vault kv put secret/trade-execution-module/fivepaisa \
  encrypt-key="kwKUJSQqOUKMtmlTeh228MLfeYdyfvcm" \
  key="4HKcWPg8bhVLGegdNXdGSh7pttmSIkjH" \
  user-id="s58VDSXoNiU" \
  login-id="52162730" \
  password="5Vhmjkgn7bs" \
  pin="161616"

# Store Telegram bot token
vault kv put secret/trade-execution-module/telegram \
  bot-token="6110276523:AAFNH9wRYkQQymniK8ioE0EnmN_6pfUZkJk" \
  chat-id="-4640817596"
```

#### **Update application.properties:**

```properties
# Remove plaintext credentials, use Vault
fivepaisa.encrypt-key=${fivepaisa.encrypt-key}
fivepaisa.key=${fivepaisa.key}
fivepaisa.user-id=${fivepaisa.user-id}
fivepaisa.login-id=${fivepaisa.login-id}
fivepaisa.password=${fivepaisa.password}
fivepaisa.pin=${fivepaisa.pin}

telegram.bot.token=${telegram.bot-token}
telegram.chat.id=${telegram.chat-id}
```

#### **Local Development Fallback:**

```yaml
# application-local.yml (for developers without Vault access)
fivepaisa:
  encrypt-key: ${FIVEPAISA_ENCRYPT_KEY:test-key}
  # ... use environment variables instead of committing
```

---

## 🔁 **FIX #9: Add Broker Order Retry with Exponential Backoff**

### **Problem:**
`TradeManager.java:272-304` - No retry on transient broker failures.

### **Solution:**

```java
package com.kotsin.execution.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.SocketTimeoutException;

/**
 * 🛡️ CRITICAL FIX #148: Retry wrapper for broker operations
 */
@Service
@Slf4j
public class BrokerRetryService {

    private final BrokerOrderService brokerOrderService;

    public BrokerRetryService(BrokerOrderService brokerOrderService) {
        this.brokerOrderService = brokerOrderService;
    }

    /**
     * Place order with automatic retry on transient failures
     * Retries: 3 attempts with exponential backoff (1s, 2s, 4s)
     */
    @Retryable(
        value = {IOException.class, SocketTimeoutException.class, BrokerTemporaryException.class},
        maxAttempts = 3,
        backoff = @Backoff(delay = 1000, multiplier = 2.0)
    )
    public String placeOrderWithRetry(String scripCode, String exchange, String exchangeType,
                                      BrokerOrderService.Side side, int quantity, double price,
                                      boolean isLimitOrder) throws BrokerException {
        log.info("Placing order (attempt in progress): scrip={} side={} qty={} price={}",
                 scripCode, side, quantity, price);

        try {
            String orderId;
            if (isLimitOrder) {
                orderId = brokerOrderService.placeLimitOrder(scripCode, exchange, exchangeType,
                                                             side, quantity, price);
            } else {
                orderId = brokerOrderService.placeMarketOrder(scripCode, exchange, exchangeType,
                                                              side, quantity);
            }

            log.info("Order placed successfully: orderId={} scrip={}", orderId, scripCode);
            return orderId;

        } catch (IOException | SocketTimeoutException e) {
            log.warn("Transient broker error (will retry): scrip={} error={}",
                     scripCode, e.getMessage());
            throw e;  // Triggers retry
        } catch (BrokerException e) {
            // Check if retryable
            if (isRetryable(e)) {
                log.warn("Retryable broker error: scrip={} error={}", scripCode, e.getMessage());
                throw new BrokerTemporaryException(e);  // Triggers retry
            } else {
                log.error("Non-retryable broker error: scrip={} error={}", scripCode, e.getMessage());
                throw e;  // No retry
            }
        }
    }

    private boolean isRetryable(BrokerException e) {
        String message = e.getMessage().toLowerCase();
        return message.contains("timeout") ||
               message.contains("temporarily unavailable") ||
               message.contains("rate limit") ||
               message.contains("503") ||
               message.contains("502");
    }

    /**
     * Exception for retryable broker errors
     */
    public static class BrokerTemporaryException extends RuntimeException {
        public BrokerTemporaryException(Throwable cause) {
            super(cause);
        }
    }
}
```

#### **Enable Retry in Spring Boot:**

Add to main application class:

```java
@SpringBootApplication
@EnableRetry  // 🛡️ Enable Spring Retry
public class TradeExecutionApplication {
    public static void main(String[] args) {
        SpringApplication.run(TradeExecutionApplication.class, args);
    }
}
```

#### **Update TradeManager to use retry:**

```java
private final BrokerRetryService brokerRetryService;

private void executeEntry(ActiveTrade trade, Candlestick confirmationCandle) {
    // ... existing code ...

    try {
        // 🛡️ CRITICAL FIX #148: Use retry service instead of direct broker call
        String orderId = brokerRetryService.placeOrderWithRetry(
            orderScrip, orderEx, orderExType, side,
            trade.getPositionSize(), limit, isOptionOrMcx
        );

        trade.addMetadata("brokerOrderId", orderId);
        log.info("Order placed with retry protection: id={}", orderId);

    } catch (BrokerException e) {
        // After all retries exhausted
        trade.addMetadata("brokerError", e.toString());
        trade.setStatus(ActiveTrade.TradeStatus.FAILED);
        log.error("Order placement failed after retries: {}", e.getMessage());

        // Send critical alert
        telegramNotificationService.sendCriticalAlert(
            "❌ Order Failed After Retries",
            String.format("Scrip: %s\nError: %s", trade.getScripCode(), e.getMessage())
        );
    }
}
```

---

## 🚧 **FIX #10: Implement Circuit Breaker for Broker Calls**

### **Problem:**
Issue #110 - Broker down → every order times out (15s * 100 orders = 25 minutes!)

### **Solution - Use Resilience4j:**

#### **Add dependency:**

```xml
<dependency>
    <groupId>io.github.resilience4j</groupId>
    <artifactId>resilience4j-spring-boot2</artifactId>
    <version>2.1.0</version>
</dependency>
```

#### **Configure circuit breaker:**

```yaml
# application.yml
resilience4j:
  circuitbreaker:
    instances:
      brokerService:
        register-health-indicator: true
        sliding-window-size: 10
        minimum-number-of-calls: 5
        failure-rate-threshold: 50
        wait-duration-in-open-state: 30000  # 30 seconds
        permitted-number-of-calls-in-half-open-state: 3
        automatic-transition-from-open-to-half-open-enabled: true
        record-exceptions:
          - java.io.IOException
          - com.kotsin.execution.broker.BrokerException
```

#### **Apply circuit breaker:**

```java
@Service
@Slf4j
public class BrokerRetryService {

    private final CircuitBreaker circuitBreaker;

    public BrokerRetryService(BrokerOrderService brokerOrderService,
                              CircuitBreakerRegistry circuitBreakerRegistry) {
        this.brokerOrderService = brokerOrderService;
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("brokerService");

        // Listen to circuit breaker events
        circuitBreaker.getEventPublisher()
            .onStateTransition(event -> {
                log.warn("🚨 Circuit breaker state changed: {} -> {}",
                         event.getStateTransition().getFromState(),
                         event.getStateTransition().getToState());

                if (event.getStateTransition().getToState() == CircuitBreaker.State.OPEN) {
                    // Send critical alert
                    sendCircuitOpenAlert();
                }
            });
    }

    @CircuitBreaker(name = "brokerService", fallbackMethod = "placeOrderFallback")
    @Retryable(...)
    public String placeOrderWithRetry(...) {
        // existing implementation
    }

    /**
     * Fallback when circuit is OPEN
     */
    private String placeOrderFallback(String scripCode, String exchange, String exchangeType,
                                      BrokerOrderService.Side side, int quantity, double price,
                                      boolean isLimitOrder, Exception e) {
        log.error("🚨 Circuit breaker OPEN - broker unavailable. Order rejected: scrip={}",
                  scripCode);

        throw new BrokerUnavailableException(
            "Broker service is currently unavailable. Circuit breaker is OPEN. " +
            "Last error: " + e.getMessage(), e
        );
    }
}
```

---

Due to length constraints, I'll create additional files for the remaining 10 critical fixes. Let me continue:
