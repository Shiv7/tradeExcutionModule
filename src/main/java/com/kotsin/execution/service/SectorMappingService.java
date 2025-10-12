package com.kotsin.execution.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Sector Mapping Service
 *
 * Loads sector classifications from CSV file.
 *
 * CRITICAL FIX: Original PortfolioRiskManager had only 10 hardcoded stocks.
 * This loads 100+ stock-sector mappings for proper correlation/sector checks.
 *
 * @author Kotsin Team
 * @version 2.0
 */
@Service
@Slf4j
public class SectorMappingService {

    private final Map<String, String> sectorMap = new HashMap<>();

    /**
     * Load sector mappings on startup
     */
    @PostConstruct
    public void initialize() {
        try {
            loadSectorMappings();
            log.info("✅ [SECTOR-MAPPING] Loaded {} stock-sector mappings", sectorMap.size());
        } catch (Exception e) {
            log.error("🚨 [SECTOR-MAPPING] Failed to load mappings: {}", e.getMessage());
            // Use fallback hardcoded mappings
            loadFallbackMappings();
        }
    }

    /**
     * Load mappings from CSV file
     */
    private void loadSectorMappings() throws Exception {
        ClassPathResource resource = new ClassPathResource("sector-mappings.csv");

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(resource.getInputStream()))) {

            String line;
            boolean isHeader = true;

            while ((line = reader.readLine()) != null) {
                if (isHeader) {
                    isHeader = false;
                    continue; // Skip header
                }

                String[] parts = line.split(",");
                if (parts.length >= 2) {
                    String symbol = parts[0].trim().toUpperCase();
                    String sector = parts[1].trim().toUpperCase();
                    sectorMap.put(symbol, sector);
                }
            }
        }

        log.info("[SECTOR-MAPPING] Loaded {} mappings from CSV", sectorMap.size());
    }

    /**
     * Fallback hardcoded mappings (if CSV load fails)
     */
    private void loadFallbackMappings() {
        // Banking
        sectorMap.put("HDFCBANK", "BANKING");
        sectorMap.put("ICICIBANK", "BANKING");
        sectorMap.put("SBIN", "BANKING");
        sectorMap.put("KOTAKBANK", "BANKING");
        sectorMap.put("AXISBANK", "BANKING");

        // IT
        sectorMap.put("TCS", "IT");
        sectorMap.put("INFY", "IT");
        sectorMap.put("WIPRO", "IT");
        sectorMap.put("HCLTECH", "IT");

        // Auto
        sectorMap.put("MARUTI", "AUTO");
        sectorMap.put("TATAMOTORS", "AUTO");
        sectorMap.put("M&M", "AUTO");

        log.warn("⚠️ [SECTOR-MAPPING] Using fallback mappings: {} entries", sectorMap.size());
    }

    /**
     * Get sector for a stock symbol
     *
     * @param symbol Stock symbol (e.g., "HDFCBANK")
     * @return Sector name (e.g., "BANKING") or "OTHER" if not found
     */
    public String getSector(String symbol) {
        if (symbol == null || symbol.isBlank()) {
            return "UNKNOWN";
        }

        String normalized = symbol.trim().toUpperCase();
        return sectorMap.getOrDefault(normalized, "OTHER");
    }

    /**
     * Check if two stocks are in the same sector
     */
    public boolean areSameSector(String symbol1, String symbol2) {
        String sector1 = getSector(symbol1);
        String sector2 = getSector(symbol2);
        return sector1.equals(sector2) && !sector1.equals("OTHER");
    }

    /**
     * Get all unique sectors
     */
    public java.util.Set<String> getAllSectors() {
        return new java.util.HashSet<>(sectorMap.values());
    }

    /**
     * Get statistics
     */
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalMappings", sectorMap.size());
        stats.put("uniqueSectors", getAllSectors().size());

        // Count stocks per sector
        Map<String, Long> sectorCounts = new HashMap<>();
        for (String sector : sectorMap.values()) {
            sectorCounts.merge(sector, 1L, Long::sum);
        }
        stats.put("sectorCounts", sectorCounts);

        return stats;
    }
}
