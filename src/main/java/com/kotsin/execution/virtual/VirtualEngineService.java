package com.kotsin.execution.virtual;

import com.kotsin.execution.virtual.model.VirtualOrder;
import com.kotsin.execution.virtual.model.VirtualPosition;
import com.kotsin.execution.virtual.model.VirtualSettings;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class VirtualEngineService {
    private final VirtualWalletRepository repo;
    private final PriceProvider prices;
    private final VirtualEventBus bus;

    public VirtualOrder createOrder(VirtualOrder req){
        long now = System.currentTimeMillis();
        req.setId(UUID.randomUUID().toString());
        req.setCreatedAt(now);
        req.setUpdatedAt(now);
        req.setStatus(VirtualOrder.Status.NEW);

        // Fill immediately for MARKET; LIMIT left pending for MVP
        if (req.getType() == VirtualOrder.Type.MARKET){
            Double ltp = prices.getLtp(req.getScripCode());
            if (ltp == null) ltp = req.getLimitPrice(); // fallback
            if (ltp == null) ltp = 0.0;
            req.setEntryPrice(ltp);
            req.setStatus(VirtualOrder.Status.FILLED);
            applyToPosition(req, ltp);
            bus.publish("order.filled", req);
        } else {
            req.setStatus(VirtualOrder.Status.PENDING);
        }
        repo.saveOrder(req);
        bus.publish("order.created", req);
        return req;
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
        p.setUpdatedAt(System.currentTimeMillis());
        repo.savePosition(p);
        bus.publish("position.updated", p);
    }

    @org.springframework.scheduling.annotation.Scheduled(fixedDelay = 500)
    void process(){
        // LIMIT fills
        for (var o : repo.listOrders(500)){
            if (o.getStatus() != VirtualOrder.Status.PENDING || o.getType()!= VirtualOrder.Type.LIMIT) continue;
            Double ltp = prices.getLtp(o.getScripCode()); if (ltp==null) continue;
            boolean hit = (o.getSide()== VirtualOrder.Side.BUY) ? ltp <= o.getLimitPrice() : ltp >= o.getLimitPrice();
            if (hit){
                o.setEntryPrice(ltp);
                o.setStatus(VirtualOrder.Status.FILLED);
                o.setUpdatedAt(System.currentTimeMillis());
                repo.saveOrder(o);
                applyToPosition(o, ltp);
                bus.publish("order.filled", o);
            }
        }

        // Triggers for positions
        for (var p : repo.listPositions()){
            Double ltp = prices.getLtp(p.getScripCode()); if (ltp==null) continue;
            boolean changed = false;
            // SL
            if (p.getQtyOpen()>0 && p.getSl()!=null){
                if (p.getSide()== VirtualPosition.Side.LONG && ltp <= p.getSl()){
                    changed |= closeAt(p, ltp, p.getQtyOpen(), false);
                    bus.publish("sl.hit", p);
                } else if (p.getSide()== VirtualPosition.Side.SHORT && ltp >= p.getSl()){
                    changed |= closeAt(p, ltp, p.getQtyOpen(), false);
                    bus.publish("sl.hit", p);
                }
            }
            if (p.getQtyOpen()==0){ repo.savePosition(p); continue; }

            // TP1
            if (Boolean.FALSE.equals(p.getTp1Hit()) && p.getTp1()!=null){
                if (p.getSide()== VirtualPosition.Side.LONG && ltp >= p.getTp1()){
                    int partial = (int)Math.max(1, Math.floor(p.getQtyOpen() * (p.getTp1ClosePercent()!=null ? p.getTp1ClosePercent() : 0.5)));
                    changed |= closeAt(p, ltp, partial, true);
                    p.setTp1Hit(true);
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
                    changed |= closeAt(p, ltp, partial, true);
                    p.setTp1Hit(true);
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
                    changed |= closeAt(p, ltp, p.getQtyOpen(), false);
                    bus.publish("tp2.hit", p);
                } else if (p.getSide()== VirtualPosition.Side.SHORT && ltp <= p.getTp2()){
                    changed |= closeAt(p, ltp, p.getQtyOpen(), false);
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
        }
    }

    private boolean closeAt(VirtualPosition p, double price, int qty, boolean partial){
        if (qty<=0) return false;
        double pnl = (p.getSide()== VirtualPosition.Side.LONG ? (price - p.getAvgEntry()) : (p.getAvgEntry() - price)) * qty;
        p.setRealizedPnl(p.getRealizedPnl() + pnl);
        int remaining = p.getQtyOpen() - qty;
        p.setQtyOpen(Math.max(0, remaining));
        if (p.getQtyOpen()==0) p.setAvgEntry(0);
        return true;
    }
}
