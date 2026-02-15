package com.kotsin.execution.virtual;

import com.kotsin.execution.paper.PaperTradeOutcomeProducer;
import com.kotsin.execution.paper.model.PaperTradeOutcome;
import com.kotsin.execution.producer.ProfitLossProducer;
import com.kotsin.execution.tracking.service.OrderStatusTracker;
import com.kotsin.execution.virtual.model.VirtualOrder;
import com.kotsin.execution.virtual.model.VirtualPosition;
import com.kotsin.execution.virtual.model.VirtualSettings;
import com.kotsin.execution.wallet.service.WalletTransactionService;
import com.kotsin.execution.wallet.service.WalletTransactionService.MarginCheckResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

@Service
@RequiredArgsConstructor
@Slf4j
public class VirtualEngineService {
    private final VirtualWalletRepository repo;
    private final PriceProvider prices;
    private final VirtualEventBus bus;

    @Autowired(required = false)
    private PaperTradeOutcomeProducer outcomeProducer;

    @Autowired(required = false)
    private ProfitLossProducer profitLossProducer;

    @Autowired(required = false)
    private WalletTransactionService walletTransactionService;

    @Autowired(required = false)
    private OrderStatusTracker orderStatusTracker;

    @Value("${wallet.enabled:true}")
    private boolean walletEnabled;

    @Value("${wallet.id:virtual-wallet-1}")
    private String defaultWalletId;

    // BUG-009 FIX: Per-scripCode locking to prevent race conditions
    private final ConcurrentHashMap<String, ReentrantLock> scripLocks = new ConcurrentHashMap<>();

    private ReentrantLock getLock(String scripCode) {
        return scripLocks.computeIfAbsent(scripCode, k -> new ReentrantLock());
    }

    /**
     * Get current open position count
     */
    private int getOpenPositionCount() {
        return (int) repo.listPositions().stream()
                .filter(p -> p.getQtyOpen() > 0)
                .count();
    }

    public VirtualOrder createOrder(VirtualOrder req){
        long now = System.currentTimeMillis();
        req.setId(UUID.randomUUID().toString());
        req.setCreatedAt(now);
        req.setUpdatedAt(now);
        req.setStatus(VirtualOrder.Status.NEW);

        // FIX: Determine price for margin calculation
        Double estimatedPrice = req.getCurrentPrice();
        if (estimatedPrice == null || estimatedPrice <= 0) {
            estimatedPrice = prices.getLtp(req.getScripCode());
        }
        if (estimatedPrice == null || estimatedPrice <= 0) {
            estimatedPrice = req.getLimitPrice();
        }
        if (estimatedPrice == null || estimatedPrice <= 0) {
            estimatedPrice = 100.0; // Fallback for margin check
        }

        // FIX: Check margin before creating order
        if (walletEnabled && walletTransactionService != null) {
            double requiredMargin = estimatedPrice * req.getQty();
            int openPositions = getOpenPositionCount();

            MarginCheckResult marginCheck = walletTransactionService.checkMarginAvailable(
                    defaultWalletId, requiredMargin, openPositions);

            if (!marginCheck.isSuccess()) {
                log.warn("❌ Order REJECTED - margin check failed: scripCode={}, qty={}, required={}, reason={}",
                        req.getScripCode(), req.getQty(), requiredMargin, marginCheck.getMessage());
                req.setStatus(VirtualOrder.Status.REJECTED);
                req.setRejectionReason(marginCheck.getMessage());
                repo.saveOrder(req);
                bus.publish("order.rejected", req);
                // Track rejection
                if (orderStatusTracker != null) {
                    orderStatusTracker.trackOrderRejected(req, marginCheck.getMessage());
                }
                return req;
            }
            log.info("✓ Margin check passed: scripCode={}, required={}, available={}",
                    req.getScripCode(), requiredMargin, marginCheck.getAvailableMargin());
        }

        // Track order creation
        if (orderStatusTracker != null) {
            orderStatusTracker.trackOrderCreated(req, defaultWalletId);
        }

        // Fill immediately for MARKET; LIMIT left pending for MVP
        if (req.getType() == VirtualOrder.Type.MARKET){
            // FIX: Try multiple fallbacks for entry price
            Double ltp = prices.getLtp(req.getScripCode());
            if (ltp == null || ltp <= 0) {
                // Try parsing scripCode if it's in N:C:18365 format
                String numericScripCode = parseNumericScripCode(req.getScripCode());
                if (!numericScripCode.equals(req.getScripCode())) {
                    ltp = prices.getLtp(numericScripCode);
                }
            }
            if (ltp == null || ltp <= 0) ltp = req.getCurrentPrice();  // FIX: Use currentPrice from UI
            if (ltp == null || ltp <= 0) ltp = req.getLimitPrice();    // Fallback to limitPrice
            if (ltp == null || ltp <= 0) {
                log.warn("⚠️ No price available for MARKET order: scripCode={}", req.getScripCode());
                ltp = 0.0;  // Last resort - but this indicates a problem
            }
            req.setEntryPrice(ltp);
            req.setStatus(VirtualOrder.Status.FILLED);
            applyToPosition(req, ltp);
            bus.publish("order.filled", req);
            // Track order fill
            if (orderStatusTracker != null) {
                orderStatusTracker.trackOrderFilled(req);
            }
            log.info("✅ MARKET order filled: scripCode={}, entryPrice={}", req.getScripCode(), ltp);
        } else {
            req.setStatus(VirtualOrder.Status.PENDING);
        }
        repo.saveOrder(req);
        bus.publish("order.created", req);
        return req;
    }

    /**
     * Parse scripCode from "N:C:18365" format to just "18365"
     */
    private String parseNumericScripCode(String scripCode) {
        if (scripCode == null) return scripCode;
        if (scripCode.contains(":")) {
            String[] parts = scripCode.split(":");
            if (parts.length >= 3) {
                return parts[2];  // Return the numeric part
            }
        }
        return scripCode;
    }

    public Optional<VirtualPosition> closePosition(String scripCode){
        Optional<VirtualPosition> posOpt = repo.getPosition(scripCode);
        if (posOpt.isEmpty()) return Optional.empty();
        VirtualPosition p = posOpt.get();
        Double ltp = prices.getLtp(scripCode);
        if (ltp == null) ltp = p.getAvgEntry();
        double pnl = (p.getSide()== VirtualPosition.Side.LONG ? (ltp - p.getAvgEntry()) : (p.getAvgEntry() - ltp)) * p.getQtyOpen();
        p.setRealizedPnl(p.getRealizedPnl() + pnl);
        p.setQtyOpen(0);
        p.setUpdatedAt(System.currentTimeMillis());
        repo.savePosition(p);
        bus.publish("position.closed", p);
        return Optional.of(p);
    }

    private void applyToPosition(VirtualOrder filled, double fill){
        VirtualPosition.Side side = filled.getSide()== VirtualOrder.Side.BUY ? VirtualPosition.Side.LONG : VirtualPosition.Side.SHORT;
        VirtualPosition p = repo.getPosition(filled.getScripCode()).orElseGet(() -> {
            VirtualPosition np = new VirtualPosition();
            np.setScripCode(filled.getScripCode());
            np.setSide(side);
            np.setQtyOpen(0);
            np.setAvgEntry(0);
            np.setRealizedPnl(0);
            np.setOpenedAt(System.currentTimeMillis());
            return np;
        });

        if (p.getQtyOpen() == 0) {
            p.setSide(side);
        }
        int q = filled.getQty();
        if (filled.getSide() == VirtualOrder.Side.BUY){
            // increase long or reduce short
            if (p.getSide()== VirtualPosition.Side.LONG){
                double newQty = p.getQtyOpen() + q;
                p.setAvgEntry((p.getAvgEntry()*p.getQtyOpen() + fill*q)/newQty);
                p.setQtyOpen((int)newQty);
            } else {
                // closing short
                int remaining = p.getQtyOpen() - q;
                if (remaining >= 0){
                    double pnl = (p.getAvgEntry() - fill) * q; // short close pnl
                    p.setRealizedPnl(p.getRealizedPnl() + pnl);
                    p.setQtyOpen(remaining);
                    if (remaining==0) p.setAvgEntry(0);
                } else {
                    // flip from short to long
                    int buyToClose = p.getQtyOpen();
                    double pnl = (p.getAvgEntry() - fill) * buyToClose;
                    p.setRealizedPnl(p.getRealizedPnl() + pnl);
                    int newLong = q - buyToClose;
                    p.setSide(VirtualPosition.Side.LONG);
                    p.setQtyOpen(newLong);
                    p.setAvgEntry(fill);
                }
            }
        } else {
            // SELL
            if (p.getSide()== VirtualPosition.Side.SHORT){
                double newQty = p.getQtyOpen() + q;
                p.setAvgEntry((p.getAvgEntry()*p.getQtyOpen() + fill*q)/newQty);
                p.setQtyOpen((int)newQty);
            } else {
                // closing long
                int remaining = p.getQtyOpen() - q;
                if (remaining >= 0){
                    double pnl = (fill - p.getAvgEntry()) * q; // long close pnl
                    p.setRealizedPnl(p.getRealizedPnl() + pnl);
                    p.setQtyOpen(remaining);
                    if (remaining==0) p.setAvgEntry(0);
                } else {
                    // flip to short
                    int sellToClose = p.getQtyOpen();
                    double pnl = (fill - p.getAvgEntry()) * sellToClose;
                    p.setRealizedPnl(p.getRealizedPnl() + pnl);
                    int newShort = q - sellToClose;
                    p.setSide(VirtualPosition.Side.SHORT);
                    p.setQtyOpen(newShort);
                    p.setAvgEntry(fill);
                }
            }
        }

        p.setSl(filled.getSl());
        p.setTp1(filled.getTp1());
        p.setTp2(filled.getTp2());
        p.setTp1ClosePercent(filled.getTp1ClosePercent());
        p.setTp1Hit(Boolean.FALSE);
        p.setTrailingType(filled.getTrailingType()==null?"NONE":filled.getTrailingType());
        p.setTrailingValue(filled.getTrailingValue());
        p.setTrailingStep(filled.getTrailingStep());
        p.setTrailingActive(Boolean.FALSE);
        p.setTrailingStop(null);
        p.setTrailAnchor(null);
        // Propagate signal metadata for quant signals
        if (filled.getSignalId() != null) {
            p.setSignalId(filled.getSignalId());
        }
        if (filled.getSignalType() != null) {
            p.setSignalType(filled.getSignalType());
        }
        if (filled.getSignalSource() != null) {
            p.setSignalSource(filled.getSignalSource());
        }
        p.setUpdatedAt(System.currentTimeMillis());
        repo.savePosition(p);
        bus.publish("position.updated", p);

        // FIX: Deduct margin from wallet when position opens/increases
        if (walletEnabled && walletTransactionService != null && filled.getQty() > 0) {
            try {
                walletTransactionService.deductMargin(
                        defaultWalletId,
                        filled.getId(),
                        p.getScripCode(),
                        p.getScripCode(), // symbol
                        filled.getSide().toString(),
                        filled.getQty(),
                        fill
                );
            } catch (Exception e) {
                log.error("Failed to deduct margin for order {}: {}", filled.getId(), e.getMessage());
            }
        }

        // BUG-002 FIX: Publish P&L entry event to Kafka
        if (profitLossProducer != null) {
            profitLossProducer.publishVirtualTradeEntry(
                p.getScripCode(),
                p.getSide().toString(),
                p.getQtyOpen(),
                p.getAvgEntry(),
                p.getSl(),
                p.getTp1()
            );
        }
    }

    @org.springframework.scheduling.annotation.Scheduled(fixedDelay = 500)
    void process(){
        // LIMIT fills
        for (var o : repo.listOrders(500)){
            if (o.getStatus() != VirtualOrder.Status.PENDING || o.getType()!= VirtualOrder.Type.LIMIT) continue;
            
            // BUG-009 FIX: Lock per scripCode to prevent race conditions
            ReentrantLock lock = getLock(o.getScripCode());
            if (!lock.tryLock()) continue; // Skip if locked by another thread
            try {
                Double ltp = prices.getLtp(o.getScripCode());
                if (ltp == null) {
                    log.debug("No price available for limit order: {}", o.getScripCode()); // BUG-013 FIX
                    continue;
                }
                boolean hit = (o.getSide()== VirtualOrder.Side.BUY) ? ltp <= o.getLimitPrice() : ltp >= o.getLimitPrice();
                if (hit){
                    o.setEntryPrice(ltp);
                    o.setStatus(VirtualOrder.Status.FILLED);
                    o.setUpdatedAt(System.currentTimeMillis());
                    repo.saveOrder(o);
                    applyToPosition(o, ltp);
                    bus.publish("order.filled", o);
                    // Track LIMIT order fill
                    if (orderStatusTracker != null) {
                        orderStatusTracker.trackOrderFilled(o);
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        // Triggers for positions
        for (var p : repo.listPositions()){
            // BUG-009 FIX: Lock per scripCode
            ReentrantLock lock = getLock(p.getScripCode());
            if (!lock.tryLock()) continue;
            try {
                Double ltp = prices.getLtp(p.getScripCode());
                if (ltp == null) {
                    log.debug("No price for position triggers: {}", p.getScripCode()); // BUG-013 FIX
                    continue;
                }
                boolean changed = false;

                // Update live price + unrealized P&L for dashboard display
                if (p.getQtyOpen() > 0) {
                    p.setCurrentPrice(ltp);
                    double uPnl = (p.getSide() == VirtualPosition.Side.LONG)
                            ? (ltp - p.getAvgEntry()) * p.getQtyOpen()
                            : (p.getAvgEntry() - ltp) * p.getQtyOpen();
                    p.setUnrealizedPnl(uPnl);
                    changed = true;
                }
            // SL
            if (p.getQtyOpen()>0 && p.getSl()!=null){
                if (p.getSide()== VirtualPosition.Side.LONG && ltp <= p.getSl()){
                    double slPnl = (ltp - p.getAvgEntry()) * p.getQtyOpen();
                    changed |= closeAt(p, ltp, p.getQtyOpen(), false, "STOP_LOSS");
                    bus.publish("sl.hit", p);
                    // Track SL hit
                    if (orderStatusTracker != null) {
                        orderStatusTracker.trackSlHit(p, ltp, slPnl);
                    }
                } else if (p.getSide()== VirtualPosition.Side.SHORT && ltp >= p.getSl()){
                    double slPnl = (p.getAvgEntry() - ltp) * p.getQtyOpen();
                    changed |= closeAt(p, ltp, p.getQtyOpen(), false, "STOP_LOSS");
                    bus.publish("sl.hit", p);
                    // Track SL hit
                    if (orderStatusTracker != null) {
                        orderStatusTracker.trackSlHit(p, ltp, slPnl);
                    }
                }
            }
            if (p.getQtyOpen()==0){ repo.savePosition(p); continue; }

            // TP1
            if (Boolean.FALSE.equals(p.getTp1Hit()) && p.getTp1()!=null){
                if (p.getSide()== VirtualPosition.Side.LONG && ltp >= p.getTp1()){
                    int partial = (int)Math.max(1, Math.floor(p.getQtyOpen() * (p.getTp1ClosePercent()!=null ? p.getTp1ClosePercent() : 0.5)));
                    double tp1Pnl = (ltp - p.getAvgEntry()) * partial;
                    changed |= closeAt(p, ltp, partial, true, "TP1_PARTIAL");
                    p.setTp1Hit(true);
                    // Track TP1 hit
                    if (orderStatusTracker != null) {
                        orderStatusTracker.trackTp1Hit(p, ltp, partial, tp1Pnl);
                    }
                    // Move SL to BE
                    if (p.getSl()==null || p.getSl() < p.getAvgEntry()) p.setSl(p.getAvgEntry());
                    // Arm trailing if configured
                    if (p.getTrailingValue()!=null && p.getTrailingType()!=null && !"NONE".equals(p.getTrailingType())){
                        p.setTrailingActive(true);
                        if ("PCT".equals(p.getTrailingType())){
                            double pct = p.getTrailingValue();
                            double ratio = pct / 100.0;
                            if (p.getSide()== VirtualPosition.Side.LONG) p.setTrailingStop(ltp * (1.0 - ratio));
                            else p.setTrailingStop(ltp * (1.0 + ratio));
                        } else { // FIXED
                            if (p.getSide()== VirtualPosition.Side.LONG) p.setTrailingStop(ltp - p.getTrailingValue());
                            else p.setTrailingStop(ltp + p.getTrailingValue());
                        }
                        p.setTrailAnchor(ltp);
                        bus.publish("trailing.armed", p);
                    }
                    bus.publish("tp1.hit", p);
                } else if (p.getSide()== VirtualPosition.Side.SHORT && ltp <= p.getTp1()){
                    int partial = (int)Math.max(1, Math.floor(p.getQtyOpen() * (p.getTp1ClosePercent()!=null ? p.getTp1ClosePercent() : 0.5)));
                    double tp1Pnl = (p.getAvgEntry() - ltp) * partial;
                    changed |= closeAt(p, ltp, partial, true, "TP1_PARTIAL");
                    p.setTp1Hit(true);
                    // Track TP1 hit
                    if (orderStatusTracker != null) {
                        orderStatusTracker.trackTp1Hit(p, ltp, partial, tp1Pnl);
                    }
                    if (p.getSl()==null || p.getSl() > p.getAvgEntry()) p.setSl(p.getAvgEntry());
                    if (p.getTrailingValue()!=null && p.getTrailingType()!=null && !"NONE".equals(p.getTrailingType())){
                        p.setTrailingActive(true);
                        if ("PCT".equals(p.getTrailingType())){
                            double pct = p.getTrailingValue();
                            double ratio = pct / 100.0;
                            if (p.getSide()== VirtualPosition.Side.LONG) p.setTrailingStop(ltp * (1.0 - ratio));
                            else p.setTrailingStop(ltp * (1.0 + ratio));
                        } else { // FIXED
                            if (p.getSide()== VirtualPosition.Side.LONG) p.setTrailingStop(ltp - p.getTrailingValue());
                            else p.setTrailingStop(ltp + p.getTrailingValue());
                        }
                        p.setTrailAnchor(ltp);
                        bus.publish("trailing.armed", p);
                    }
                    bus.publish("tp1.hit", p);
                }
            }

            if (p.getQtyOpen()==0){ repo.savePosition(p); continue; }

            // TP2
            if (p.getTp2()!=null){
                if (p.getSide()== VirtualPosition.Side.LONG && ltp >= p.getTp2()){
                    changed |= closeAt(p, ltp, p.getQtyOpen(), false, "TARGET_HIT");
                    bus.publish("tp2.hit", p);
                } else if (p.getSide()== VirtualPosition.Side.SHORT && ltp <= p.getTp2()){
                    changed |= closeAt(p, ltp, p.getQtyOpen(), false, "TARGET_HIT");
                    bus.publish("tp2.hit", p);
                }
            }

            if (p.getQtyOpen()==0){ repo.savePosition(p); continue; }

            // Trailing (FIXED and PCT)
            if (Boolean.TRUE.equals(p.getTrailingActive()) && p.getTrailingValue()!=null){
                boolean isPct = "PCT".equals(p.getTrailingType());
                if (p.getSide()== VirtualPosition.Side.LONG){
                    if (p.getTrailAnchor()==null || ltp > p.getTrailAnchor()){
                        double newStop = isPct ? ltp * (1.0 - (p.getTrailingValue()/100.0)) : (ltp - p.getTrailingValue());
                        boolean passStep = (p.getTrailingStop()==null) || (p.getTrailingStep()==null) || (newStop - p.getTrailingStop() >= p.getTrailingStep());
                        if (passStep){
                            p.setTrailingStop(newStop);
                            p.setSl(Math.max(p.getSl()!=null?p.getSl():newStop, newStop));
                            p.setTrailAnchor(ltp);
                            bus.publish("trailing.update", p);
                            changed = true;
                        }
                    }
                } else { // SHORT
                    if (p.getTrailAnchor()==null || ltp < p.getTrailAnchor()){
                        double newStop = isPct ? ltp * (1.0 + (p.getTrailingValue()/100.0)) : (ltp + p.getTrailingValue());
                        boolean passStep = (p.getTrailingStop()==null) || (p.getTrailingStep()==null) || (p.getTrailingStop() - newStop >= p.getTrailingStep());
                        if (passStep){
                            p.setTrailingStop(newStop);
                            p.setSl(Math.min(p.getSl()!=null?p.getSl():newStop, newStop));
                            p.setTrailAnchor(ltp);
                            bus.publish("trailing.update", p);
                            changed = true;
                        }
                    }
                }
            }

            if (changed){ p.setUpdatedAt(System.currentTimeMillis()); repo.savePosition(p); bus.publish("position.updated", p);}
            } finally {
                lock.unlock(); // BUG-009 FIX: Always release lock
            }
        }
    }

    private boolean closeAt(VirtualPosition p, double price, int qty, boolean partial){
        return closeAt(p, price, qty, partial, null);
    }
    
    private boolean closeAt(VirtualPosition p, double price, int qty, boolean partial, String exitReason){
        if (qty<=0) return false;
        double pnl = (p.getSide()== VirtualPosition.Side.LONG ? (price - p.getAvgEntry()) : (p.getAvgEntry() - price)) * qty;
        p.setRealizedPnl(p.getRealizedPnl() + pnl);
        int remaining = p.getQtyOpen() - qty;
        p.setQtyOpen(Math.max(0, remaining));

        // FIX: Credit P&L to wallet when position closes
        if (walletEnabled && walletTransactionService != null) {
            try {
                walletTransactionService.creditPnl(
                        defaultWalletId,
                        p.getScripCode() + "_" + p.getOpenedAt(),
                        p.getScripCode(),
                        p.getScripCode(), // symbol
                        p.getSide().toString(),
                        qty,
                        p.getAvgEntry(),
                        price,
                        exitReason != null ? exitReason : "UNKNOWN"
                );
                log.info("WALLET_PNL_CREDITED scrip={} pnl={} exit={}", p.getScripCode(), pnl, exitReason);
            } catch (Exception e) {
                log.error("Failed to credit P&L for position {}: {}", p.getScripCode(), e.getMessage());
            }
        }

        // If position fully closed and has signalId, send outcome to StreamingCandle
        if (p.getQtyOpen() == 0 && p.getSignalId() != null && outcomeProducer != null) {
            sendOutcome(p, price, pnl, exitReason);
        }

        // Track position close
        if (p.getQtyOpen() == 0 && orderStatusTracker != null) {
            orderStatusTracker.trackPositionClosed(p, price, pnl, exitReason);
        }

        // BUG-002 FIX: Publish P&L exit event to Kafka for dashboard
        if (profitLossProducer != null) {
            profitLossProducer.publishVirtualTradeExit(
                p.getScripCode(),
                p.getSide().toString(),
                qty,
                p.getAvgEntry(),
                price,
                pnl,
                exitReason != null ? exitReason : "UNKNOWN"
            );
        }

        if (p.getQtyOpen()==0) p.setAvgEntry(0);
        return true;
    }
    
    /**
     * Send trade outcome to StreamingCandle for stats update
     */
    private void sendOutcome(VirtualPosition p, double exitPrice, double pnl, String exitReason) {
        try {
            // Calculate R-multiple
            double risk = Math.abs(p.getAvgEntry() - (p.getSl() != null ? p.getSl() : p.getAvgEntry()));
            double rMultiple = risk > 0 ? pnl / (risk * p.getQtyOpen()) : 0;
            
            // Determine direction
            String direction = p.getSide() == VirtualPosition.Side.LONG ? "BULLISH" : "BEARISH";
            
            // Calculate holding period
            long holdingMinutes = (System.currentTimeMillis() - p.getOpenedAt()) / 60000;
            
            PaperTradeOutcome outcome = PaperTradeOutcome.builder()
                    .id(UUID.randomUUID().toString())
                    .signalId(p.getSignalId())
                    .scripCode(p.getScripCode())
                    .signalType(p.getSignalType() != null ? p.getSignalType() : "BREAKOUT_RETEST")
                    .direction(direction)
                    .entryPrice(p.getAvgEntry())
                    .exitPrice(exitPrice)
                    .stopLoss(p.getSl() != null ? p.getSl() : 0)
                    .target(p.getTp1() != null ? p.getTp1() : 0)
                    .quantity(p.getQtyOpen())
                    .exitReason(exitReason != null ? exitReason : "UNKNOWN")
                    .pnl(pnl)
                    .rMultiple(rMultiple)
                    .win(pnl > 0)
                    .entryTime(LocalDateTime.ofInstant(Instant.ofEpochMilli(p.getOpenedAt()), ZoneId.of("Asia/Kolkata")))
                    .exitTime(LocalDateTime.now(ZoneId.of("Asia/Kolkata")))
                    .holdingPeriodMinutes(holdingMinutes)
                    .positionSizeMultiplier(p.getPositionSizeMultiplier())
                    .build();
            
            outcomeProducer.send(outcome);
            
        } catch (Exception e) {
            log.error("Failed to send outcome for {}: {}", p.getScripCode(), e.getMessage());
        }
    }
}
