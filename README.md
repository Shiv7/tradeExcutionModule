# 🚀 Kotsin Real-Time Trade Execution Module

## 🎯 **Overview**

The **Trade Execution Module** is a real-time trading engine that consumes signals from strategy modules, monitors live market data, and executes simulated trades with sophisticated risk management. When trades close, it publishes **final profit/loss results** to a dedicated Kafka topic.

## 🏗️ **Architecture**

### **Data Flow**
```
Strategy Signals → Trade Creation → Live Market Monitoring → Exit Detection → Profit/Loss Publishing
       ↓                ↓                    ↓                    ↓                  ↓
   Signal Consumer → Trade Manager → Market Data Consumer → Exit Service → Result Producer
```

### **Kafka Topics**

#### **Input Topics (Consumed)**
- `bb-supertrend-signals` - BB SuperTrend strategy signals
- `supertrend-break-signals` - SuperTrend Break strategy signals  
- `three-minute-supertrend-signals` - 3-minute SuperTrend signals
- `fudkii_Signal` - Custom Fudkii strategy signals
- `forwardtesting-data` - Real-time market tick data

#### **Output Topics (Published)**
- `trade-results` - **Final trade results with profit/loss** 🎯
- `daily-trade-summary` - Daily trading performance summaries

## 📊 **Trade Result Schema**

When trades close, the following comprehensive data is published to `trade-results`:

```json
{
  "tradeId": "BANKNIFTY_BB_SUPERTREND_2024010915_a1b2c3d4",
  "scripCode": "BANKNIFTY",
  "companyName": "NIFTY BANK",
  "exchange": "N",
  "exchangeType": "DERIVATIVES",
  "strategyName": "BB_SUPERTREND",
  "signalType": "BULLISH",
  "signalTime": "2024-01-09T15:30:00",
  
  // Entry & Exit
  "entryPrice": 47250.0,
  "entryTime": "2024-01-09T15:31:00",
  "exitPrice": 47500.0,
  "exitTime": "2024-01-09T16:15:00",
  "exitReason": "TARGET_2",
  
  // Financial Results
  "profitLoss": 2500.0,
  "roi": 5.29,
  "successful": true,
  "riskAdjustedReturn": 2.5,
  
  // Performance Metrics
  "target1Hit": true,
  "target2Hit": true,
  "durationMinutes": 44,
  "maxFavorableExcursion": 3000.0,
  "maxAdverseExcursion": -500.0,
  
  // Risk Management
  "initialStopLoss": 47000.0,
  "finalStopLoss": 47125.0,
  "riskAmount": 1000.0,
  
  // Timestamps
  "resultGeneratedTime": "2024-01-09T16:15:01",
  "systemVersion": "1.0.0"
}
```

## 🔧 **Core Components**

### **1. StrategySignalConsumer**
- Listens to all strategy module topics
- Parses signal data and determines bullish/bearish direction
- Validates signal completeness
- Hands over to TradeExecutionService

### **2. TradeExecutionService**
- Processes new signals and creates active trades
- Manages trade lifecycle from signal to closure
- Calculates entry/exit conditions
- Orchestrates risk management

### **3. LiveMarketDataConsumer**
- Consumes real-time ticks from `forwardtesting-data`
- Updates active trades with current market prices
- Triggers entry/exit condition checks

### **4. TradeResultProducer**
- Publishes final trade results to `trade-results` topic
- Calculates comprehensive performance metrics
- Provides daily summary statistics

### **5. TradeStateManager**
- Manages active trades in memory/Redis
- Prevents duplicate trades per script+strategy
- Provides fast lookups for price updates

### **6. RiskManager**
- Calculates position sizes based on risk parameters
- Sets stop-loss and target levels
- Manages trailing stops after target hits

## ⚙️ **Configuration**

### **Risk Management Settings**
```yaml
app:
  trading:
    risk:
      max-risk-per-trade: 1.0      # 1% max risk per trade
      default-position-size: 1000   # Default position size
      max-position-size: 10000     # Maximum position size
      
    management:
      max-active-trades-per-script: 1  # One trade per script
      max-holding-days: 5             # 5-day max holding
      entry-buffer-percent: 0.1       # 0.1% entry buffer
      
    targets:
      target1-multiplier: 1.5    # 1.5R first target
      target2-multiplier: 2.5    # 2.5R second target
      target3-multiplier: 4.0    # 4.0R third target
      target4-multiplier: 6.0    # 6.0R fourth target
```

## 🚀 **Quick Start**

### **1. Prerequisites**
- Java 17+
- Apache Kafka running on localhost:9092
- Redis running on localhost:6379
- Strategy modules publishing signals

### **2. Build & Run**
```bash
# Clone and build
cd tradeExecutionModule
mvn clean package

# Run the application
java -jar target/trade-execution-module-1.0.0.jar

# Or with Maven
mvn spring-boot:run
```

### **3. Monitor Trade Results**
```bash
# Listen to trade results topic
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic trade-results --from-beginning

# Listen to daily summaries
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic daily-trade-summary --from-beginning
```

## 📈 **Trade Execution Logic**

### **Signal Processing**
1. **Signal Reception**: Consume from strategy topics
2. **Validation**: Check for required fields (scripCode, signal type)
3. **Duplicate Check**: Ensure no active trade for script+strategy
4. **Trade Creation**: Calculate risk levels and create ActiveTrade

### **Entry Logic**
- **Bullish**: Enter when price moves up 0.1% from signal price
- **Bearish**: Enter when price moves down 0.1% from signal price
- **Immediate**: Set entry price and start monitoring exits

### **Exit Logic**
1. **Stop Loss**: Price hits initial or trailing stop
2. **Target 1**: Activate trailing stop, continue monitoring
3. **Target 2**: Close trade with profit
4. **Time Limit**: Close after 5 days maximum holding

### **Trailing Stop**
- Activated after Target 1 hit
- Moves stop to 50% of initial stop distance
- Protects profits while allowing further upside

## 🎯 **Key Features**

### **✅ Real-Time Execution**
- Live market data integration via `forwardtesting-data`
- Tick-by-tick price monitoring
- Immediate exit detection

### **✅ Sophisticated Risk Management**
- Position sizing based on risk percentage
- Multiple target levels (1.5R, 2.5R, 4.0R, 6.0R)
- Trailing stops after first target
- Maximum holding period protection

### **✅ Comprehensive Results**
- **Profit/Loss calculation** with fees
- **ROI and risk-adjusted returns**
- **Maximum favorable/adverse excursions**
- **Trade duration and success metrics**

### **✅ Strategy Integration**
- Supports all existing strategy modules
- BB SuperTrend, SuperTrend Break, 3-minute strategies
- Extensible for new strategy types

### **✅ Monitoring & Observability**
- Detailed logging with trade lifecycle events
- Prometheus metrics for performance monitoring
- Health checks and application status

## 📊 **Performance Metrics**

The module tracks and publishes:

- **Individual Trade Results**: P&L, ROI, duration, exit reason
- **Daily Summaries**: Total trades, win rate, total P&L
- **Risk Metrics**: Maximum drawdown, Sharpe ratio
- **Execution Metrics**: Entry/exit timing, slippage

## 🔍 **Monitoring**

### **Health Check**
```bash
curl http://localhost:8080/actuator/health
```

### **Metrics**
```bash
curl http://localhost:8080/actuator/metrics
```

### **Logs**
```bash
tail -f logs/trade-execution.log
```

## 🎯 **Integration with Existing System**

### **Dependencies**
- **indicatorAgg**: Redis indicator data for trade validation
- **strategyModule**: Signal sources for trade creation
- **optionProducer**: Real-time market data via `forwardtesting-data`

### **Outputs**
- **trade-results**: Consumed by analytics/reporting systems
- **daily-trade-summary**: Used for performance dashboards

## 🛠️ **Development**

### **Adding New Strategy Support**
1. Add new topic listener in `StrategySignalConsumer`
2. Implement signal type detection logic
3. Update configuration if needed

### **Custom Exit Logic**
1. Extend `TradeExecutionService.checkExitConditions()`
2. Add new exit reasons and logic
3. Update trade result publishing

### **Enhanced Risk Management**
1. Modify `RiskManager.calculateTradeLevels()`
2. Add new risk parameters to configuration
3. Update position sizing algorithms

---

## 🎯 **Final Result: Profit/Loss Publishing**

**The core requirement is fulfilled**: When any trade closes (profit, loss, or time limit), the module publishes comprehensive trade results with **final profit/loss** to the `trade-results` Kafka topic, enabling real-time performance tracking and analysis.

This architecture provides a production-ready real-time trading execution engine that integrates seamlessly with the existing Kotsin trading system while adding sophisticated trade management and comprehensive result reporting. 