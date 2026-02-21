package com.kotsin.execution.quant.service;

import com.kotsin.execution.model.ActiveTrade;
import com.kotsin.execution.quant.model.QuantTradingSignal;
import com.kotsin.execution.service.PortfolioRiskManager;
import com.kotsin.execution.service.WalletService;
import com.kotsin.execution.virtual.VirtualEngineService;
import com.kotsin.execution.virtual.VirtualWalletRepository;
import com.kotsin.execution.virtual.model.VirtualOrder;
import com.kotsin.execution.virtual.model.VirtualPosition;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * QuantSignalRouter - Routes QuantTradingSignal to VirtualEngineService.
 *
 * Applies risk gates via PortfolioRiskManager before execution:
 * - Position count limits
 * - Sector concentration
 * - Drawdown limits
 * - Leverage limits
 *
 * Also handles hedging orders when recommended.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class QuantSignalRouter {

    private final VirtualEngineService virtualEngine;
    private final VirtualWalletRepository walletRepository;
    private final PortfolioRiskManager riskManager;
    private final QuantPositionSizer positionSizer;
    private final WalletService walletService;

    @Value("${quant.signals.enable-hedging:true}")
    private boolean enableHedging;

    @Value("${quant.signals.max-position-value:500000}")
    private double maxPositionValue;

    /**
     * Route signal to VirtualEngine with risk checks
     */
    public void routeSignal(QuantTradingSignal signal) {
        String scripCode = signal.getScripCode();

        try {
            // ========== Check for Existing Position ==========
            Optional<VirtualPosition> existingPos = walletRepository.getPosition(scripCode);
            if (existingPos.isPresent() && existingPos.get().getQtyOpen() > 0) {
                log.info("quant_position_exists scrip={} qty={} side={}",
                        scripCode,
                        existingPos.get().getQtyOpen(),
                        existingPos.get().getSide());

                // Check if signal is in opposite direction - might want to close
                VirtualPosition pos = existingPos.get();
                boolean positionLong = pos.getSide() == VirtualPosition.Side.LONG;
                boolean signalLong = signal.isLong();

                if (positionLong != signalLong) {
                    log.info("quant_signal_opposite_direction scrip={} closing existing position", scripCode);
                    virtualEngine.closePosition(scripCode);
                }
                return;
            }

            // ========== Build ActiveTrade for Risk Check ==========
            int quantity = calculateQuantity(signal);
            if (quantity <= 0) {
                log.warn("quant_invalid_quantity scrip={} qty={}", scripCode, quantity);
                return;
            }

            ActiveTrade proposedTrade = ActiveTrade.builder()
                    .scripCode(scripCode)
                    .companyName(signal.getCompanyName())
                    .entryPrice(signal.getEntryPrice())
                    .positionSize(quantity)
                    .stopLoss(signal.getStopLoss())
                    .target1(signal.getTarget1())
                    .target2(signal.getTarget2())
                    .signalType(signal.isLong() ? "BULLISH" : "BEARISH")
                    .build();

            // ========== Risk Gate Check ==========
            List<ActiveTrade> currentPositions = getCurrentPositions();

            if (!riskManager.canTakeTrade(proposedTrade, currentPositions)) {
                log.warn("quant_risk_gate_blocked scrip={} score={}",
                        scripCode, signal.getQuantScore());
                return;
            }

            // ========== Create Virtual Order ==========
            VirtualOrder order = buildVirtualOrder(signal, quantity);
            VirtualOrder filledOrder = virtualEngine.createOrder(order);

            log.info("QUANT_ORDER_CREATED id={} scrip={} side={} qty={} entry={} sl={} tp1={}",
                    filledOrder.getId(),
                    scripCode,
                    filledOrder.getSide(),
                    quantity,
                    String.format("%.2f", signal.getEntryPrice()),
                    String.format("%.2f", signal.getStopLoss()),
                    String.format("%.2f", signal.getTarget1()));

            // ========== Handle Hedging ==========
            if (enableHedging && signal.getHedging() != null && signal.getHedging().isRecommended()) {
                createHedgingOrder(signal, filledOrder);
            }

            // ========== Record Order in Wallet ==========
            // FIX: Handle WalletService exceptions properly instead of silently ignoring
            try {
                String walletOrderId = walletService.recordOrder(
                    scripCode,
                    signal.isLong() ? "BUY" : "SELL",
                    quantity,
                    signal.getEntryPrice(),
                    "QUANT"
                );
                log.info("QUANT_WALLET_RECORDED orderId={} scrip={}", walletOrderId, scripCode);
            } catch (WalletService.WalletOperationException we) {
                // Log wallet failure but don't fail the trade
                // The virtual order was already created successfully
                log.error("QUANT_WALLET_FAILED scrip={} orderId={} error={}",
                        scripCode, filledOrder.getId(), we.getMessage());
            }

        } catch (Exception e) {
            log.error("quant_route_error scrip={} err={}", scripCode, e.getMessage(), e);
        }
    }

    /**
     * Calculate position quantity from signal
     */
    private int calculateQuantity(QuantTradingSignal signal) {
        // Use sizing from signal if available
        if (signal.getSizing() != null && signal.getSizing().getQuantity() > 0) {
            return signal.getSizing().getQuantity();
        }

        // Calculate from position sizer
        return positionSizer.calculateQuantity(
                signal.getEntryPrice(),
                signal.getStopLoss(),
                signal.getConfidence(),
                signal.getSizing() != null ? signal.getSizing().getPositionSizeMultiplier() : 1.0
        );
    }

    /**
     * Build VirtualOrder from signal
     */
    private VirtualOrder buildVirtualOrder(QuantTradingSignal signal, int quantity) {
        VirtualOrder order = new VirtualOrder();

        order.setScripCode(signal.getScripCode());
        order.setSide(signal.isLong() ? VirtualOrder.Side.BUY : VirtualOrder.Side.SELL);
        order.setType(VirtualOrder.Type.MARKET);
        order.setQty(quantity);
        order.setLimitPrice(signal.getEntryPrice());

        // Stop loss and targets
        order.setSl(signal.getStopLoss());
        order.setTp1(signal.getTarget1());
        order.setTp2(signal.getTarget2());
        order.setTp1ClosePercent(0.5);  // Close 50% at TP1

        // Trailing stop configuration
        if (signal.getTrailingStop() != null && signal.getTrailingStop().isEnabled()) {
            var ts = signal.getTrailingStop();
            order.setTrailingType(ts.getType());
            order.setTrailingValue(ts.getValue());
            order.setTrailingStep(ts.getStep());
        } else {
            order.setTrailingType("NONE");
        }

        // Signal metadata
        order.setSignalId(signal.getSignalId());
        order.setSignalType(signal.getSignalType() != null ? signal.getSignalType().name() : "QUANT");
        order.setSignalSource("QUANT");
        order.setExchange(signal.getExchange() != null ? signal.getExchange() : "N");
        order.setRationale(signal.getRationale());
        order.setInstrumentSymbol(signal.getCompanyName());

        return order;
    }

    /**
     * Create hedging order if recommended
     */
    private void createHedgingOrder(QuantTradingSignal signal, VirtualOrder mainOrder) {
        var hedge = signal.getHedging();

        try {
            VirtualOrder hedgeOrder = new VirtualOrder();

            hedgeOrder.setScripCode(hedge.getHedgeInstrument());
            hedgeOrder.setQty(hedge.getHedgeQuantity());
            hedgeOrder.setType(VirtualOrder.Type.MARKET);

            // Hedging is opposite direction to main position
            if (signal.isLong()) {
                hedgeOrder.setSide(VirtualOrder.Side.SELL);
            } else {
                hedgeOrder.setSide(VirtualOrder.Side.BUY);
            }

            hedgeOrder.setSignalId(signal.getSignalId() + "_HEDGE");
            hedgeOrder.setSignalType("HEDGE_" + hedge.getHedgeType());
            hedgeOrder.setRationale(hedge.getHedgeRationale());

            VirtualOrder filledHedge = virtualEngine.createOrder(hedgeOrder);

            log.info("QUANT_HEDGE_CREATED id={} instrument={} side={} qty={} ratio={}",
                    filledHedge.getId(),
                    hedge.getHedgeInstrument(),
                    hedgeOrder.getSide(),
                    hedge.getHedgeQuantity(),
                    hedge.getHedgeRatio());

        } catch (Exception e) {
            log.error("quant_hedge_error instrument={} err={}",
                    hedge.getHedgeInstrument(), e.getMessage());
        }
    }

    /**
     * Get current virtual positions as ActiveTrade list
     */
    private List<ActiveTrade> getCurrentPositions() {
        List<ActiveTrade> trades = new ArrayList<>();

        for (VirtualPosition pos : walletRepository.listPositions()) {
            if (pos.getQtyOpen() > 0) {
                ActiveTrade trade = ActiveTrade.builder()
                        .scripCode(pos.getScripCode())
                        .entryPrice(pos.getAvgEntry())
                        .positionSize(pos.getQtyOpen())
                        .stopLoss(pos.getSl())
                        .target1(pos.getTp1())
                        .signalType(pos.getSide() == VirtualPosition.Side.LONG ? "BULLISH" : "BEARISH")
                        .build();
                trades.add(trade);
            }
        }

        return trades;
    }
}
