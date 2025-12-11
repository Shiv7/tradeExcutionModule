package com.kotsin.execution.virtual;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kotsin.execution.virtual.model.VirtualOrder;
import com.kotsin.execution.virtual.model.VirtualPosition;
import com.kotsin.execution.virtual.model.VirtualSettings;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

import java.util.*;

@Repository
@RequiredArgsConstructor
@Slf4j
public class VirtualWalletRepository {
    private final RedisTemplate<String, String> executionStringRedisTemplate;
    private final ObjectMapper mapper = new ObjectMapper();

    private String orderKey(String id){ return "virtual:orders:"+id; }
    private String posKey(String scrip){ return "virtual:positions:"+scrip; }
    private String settingsKey(){ return "virtual:settings"; }

    public void saveOrder(VirtualOrder o) {
        try { executionStringRedisTemplate.opsForValue().set(orderKey(o.getId()), mapper.writeValueAsString(o)); }
        catch (Exception e){ log.warn("saveOrder failed: {}", e.getMessage()); }
    }

    public Optional<VirtualOrder> getOrder(String id) {
        try {
            String raw = executionStringRedisTemplate.opsForValue().get(orderKey(id));
            return raw == null ? Optional.empty() : Optional.of(mapper.readValue(raw, VirtualOrder.class));
        } catch (Exception e){ return Optional.empty(); }
    }

    public List<VirtualOrder> listOrders(int max) {
        List<VirtualOrder> out = new ArrayList<>();
        try (var c = executionStringRedisTemplate.scan(org.springframework.data.redis.core.ScanOptions.scanOptions().match("virtual:orders:*").count(1000).build())) {
            while (c.hasNext() && out.size() < max){
                String raw = executionStringRedisTemplate.opsForValue().get(c.next());
                if (raw != null) try { out.add(mapper.readValue(raw, VirtualOrder.class)); } catch (Exception e) { log.warn("Failed to parse order: {}", e.getMessage()); }
            }
        } catch (Exception e) { log.error("Failed to list orders from Redis: {}", e.getMessage()); }
        out.sort(Comparator.comparingLong(VirtualOrder::getCreatedAt).reversed());
        return out;
    }

    public void savePosition(VirtualPosition p){
        try { executionStringRedisTemplate.opsForValue().set(posKey(p.getScripCode()), mapper.writeValueAsString(p)); }
        catch (Exception e){ log.warn("savePosition failed: {}", e.getMessage()); }
    }

    public Optional<VirtualPosition> getPosition(String scrip){
        try {
            String raw = executionStringRedisTemplate.opsForValue().get(posKey(scrip));
            return raw == null ? Optional.empty() : Optional.of(mapper.readValue(raw, VirtualPosition.class));
        } catch (Exception e){ return Optional.empty(); }
    }

    public List<VirtualPosition> listPositions(){
        List<VirtualPosition> out = new ArrayList<>();
        try (var c = executionStringRedisTemplate.scan(org.springframework.data.redis.core.ScanOptions.scanOptions().match("virtual:positions:*").count(1000).build())) {
            while (c.hasNext()){
                String raw = executionStringRedisTemplate.opsForValue().get(c.next());
                if (raw != null) try { out.add(mapper.readValue(raw, VirtualPosition.class)); } catch (Exception e) { log.warn("Failed to parse position: {}", e.getMessage()); }
            }
        } catch (Exception e) { log.error("Failed to list positions from Redis: {}", e.getMessage()); }
        return out;
    }

    public VirtualSettings loadSettings(){
        try { String raw = executionStringRedisTemplate.opsForValue().get(settingsKey());
            return raw == null ? new VirtualSettings() : mapper.readValue(raw, VirtualSettings.class);
        } catch (Exception e){ return new VirtualSettings(); }
    }
    public void saveSettings(VirtualSettings s){
        try { executionStringRedisTemplate.opsForValue().set(settingsKey(), mapper.writeValueAsString(s)); } catch (Exception e) { log.error("Failed to save settings: {}", e.getMessage()); }
    }
}

