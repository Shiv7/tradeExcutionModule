package com.kotsin.execution.paper;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kotsin.execution.virtual.PriceProvider;
import com.kotsin.execution.virtual.VirtualEngineService;
import com.kotsin.execution.virtual.VirtualWalletRepository;
import com.kotsin.execution.virtual.model.VirtualOrder;
import com.kotsin.execution.virtual.model.VirtualPosition;
import com.kotsin.execution.virtual.model.VirtualSettings;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * CuratedSignalConsumer - Consumes curated signals and creates paper trades
 * 
 * Listens to: trading-signals-curated
 * Creates: VirtualOrder via VirtualEngineService
 */
@Component
@Slf4j
public class CuratedSignalConsumer {

    @Autowired
    private VirtualEngineService virtualEngine;

    @Autowired
    private VirtualWalletRepository walletRepo;

    @Autowired
    private PriceProvider priceProvider;

    @Value("${paper.trade.enabled:true}")
    private boolean enabled;

    @Value("${paper.trade.max.positions:5}")
    private int maxPositions;

    @Value("${paper.trade.position.percent:2.0}")
    private double positionPercent;

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    /**
     * Minimal CuratedSignal DTO for consumption (avoid circular dependency)
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class CuratedSignalDTO {
        private String scripCode;
        private String companyName;
        private long timestamp;
        private String signalId;
        private double curatedScore;
        private double positionSizeMultiplier;
        private EntryDTO entry;

        @Data
        @NoArgsConstructor
        @AllArgsConstructor
        @JsonIgnoreProperties(ignoreUnknown = true)
        public static class EntryDTO {
            private double entryPrice;
            private double stopLoss;
            private double target;
            private double riskReward;
            private double positionSizeMultiplier;
        }
    }

    // DISABLED: This consumer is redundant - QuantSignalConsumer already handles trading-signals-v2
    // The CuratedSignalDTO format doesn't match QuantTradingSignal format in trading-signals-v2
    // Keeping the code for reference but disabling the listener
    // @KafkaListener(
    //         topics = "trading-signals-v2",
    //         groupId = "paper-trade-executor-curated",
    //         containerFactory = "kafkaListenerContainerFactory"
    // )
    public void onCuratedSignal(String payload) {
        // DISABLED - QuantSignalConsumer handles this topic
        if (!enabled) {
            return;
        }

        try {
            CuratedSignalDTO signal = MAPPER.readValue(payload, CuratedSignalDTO.class);
            if (signal == null || signal.getScripCode() == null) {
                log.warn("Invalid curated signal received");
                return;
            }

            // Check if we already have a position for this scrip
            if (walletRepo.getPosition(signal.getScripCode()).filter(p -> p.getQtyOpen() > 0).isPresent()) {
                log.debug("Already have open position for {}, skipping signal", signal.getScripCode());
                return;
            }

            // Check max positions
            long openPositions = walletRepo.listPositions().stream()
                    .filter(p -> p.getQtyOpen() > 0)
                    .count();
            if (openPositions >= maxPositions) {
                log.debug("Max positions ({}) reached, skipping signal for {}", maxPositions, signal.getScripCode());
                return;
            }

            // Calculate position size
            VirtualSettings settings = walletRepo.loadSettings();
            double accountValue = settings.getAccountValue();
            double rawMultiplier = signal.getPositionSizeMultiplier();
            final double multiplier = rawMultiplier <= 0 ? 1.0 : rawMultiplier;
            
            double positionValue = accountValue * (positionPercent / 100.0) * multiplier;
            
            // Get current price
            Double currentPrice = priceProvider.getLtp(signal.getScripCode());
            if (currentPrice == null || currentPrice <= 0) {
                currentPrice = signal.getEntry() != null ? signal.getEntry().getEntryPrice() : 0;
            }
            if (currentPrice <= 0) {
                log.warn("No price available for {}, skipping", signal.getScripCode());
                return;
            }

            int quantity = (int) Math.floor(positionValue / currentPrice);
            if (quantity < 1) {
                log.warn("Position too small for {}, skipping", signal.getScripCode());
                return;
            }

            // Determine direction from entry (target > entry = bullish)
            boolean isBullish = true;
            if (signal.getEntry() != null) {
                isBullish = signal.getEntry().getTarget() > signal.getEntry().getEntryPrice();
            }

            // Create virtual order
            VirtualOrder order = new VirtualOrder();
            order.setScripCode(signal.getScripCode());
            order.setSide(isBullish ? VirtualOrder.Side.BUY : VirtualOrder.Side.SELL);
            order.setType(VirtualOrder.Type.MARKET);
            order.setQty(quantity);
            
            if (signal.getEntry() != null) {
                order.setSl(signal.getEntry().getStopLoss());
                order.setTp1(signal.getEntry().getTarget());
                // Use TP1 at 50% position, rest trail or go to TP2
                order.setTp1ClosePercent(0.5);
                // Set trailing after TP1
                order.setTrailingType("PCT");
                order.setTrailingValue(1.0);  // 1% trailing
                order.setTrailingStep(0.5);   // 0.5 point step
            }

            // Store signalId for outcome tracking (handled by enhanced VirtualPosition)
            // Note: VirtualEngineService.applyToPosition will need enhancement to pass this through

            VirtualOrder executed = virtualEngine.createOrder(order);

            // Update the position with signalId
            walletRepo.getPosition(signal.getScripCode()).ifPresent(position -> {
                position.setSignalId(signal.getSignalId());
                position.setSignalType("BREAKOUT_RETEST");
                position.setPositionSizeMultiplier(multiplier);
                walletRepo.savePosition(position);
            });

            log.info("PAPER TRADE ENTRY | {} | side={} | qty={} | price={} | SL={} | TP={} | signalId={}",
                    signal.getScripCode(),
                    order.getSide(),
                    quantity,
                    String.format("%.2f", currentPrice),
                    String.format("%.2f", signal.getEntry() != null ? signal.getEntry().getStopLoss() : 0),
                    String.format("%.2f", signal.getEntry() != null ? signal.getEntry().getTarget() : 0),
                    signal.getSignalId());

        } catch (Exception e) {
            log.error("Failed to process curated signal: {}", e.getMessage(), e);
        }
    }
}

