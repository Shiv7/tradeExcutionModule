package com.kotsin.execution.service;

import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LOT-SIZE: Provides lot size and multiplier lookups for all instrument types.
 *
 * Data sources:
 * 1. scripData collection — direct lookup by scripCode (MCX, Currency, Index derivatives)
 * 2. ScripGroup collection — equity→FUT/Options lot sizes (company-specific NSE F&O)
 *
 * All qty calculations MUST use roundToLotSize() to ensure lot-size compliance.
 */
@Service
@Slf4j
public class LotSizeLookupService {

    @Autowired
    private MongoTemplate mongoTemplate;

    private final ConcurrentHashMap<String, LotSizeInfo> cache = new ConcurrentHashMap<>();

    @PostConstruct
    public void preloadCache() {
        int count = 0;
        try {
            // Load from scripData collection (covers MCX, Currency, Index, NSE F&O, Equity)
            // Fields are at top level: ScripCode, LotSize, Multiplier
            for (Document doc : mongoTemplate.getCollection("scripData").find()) {
                try {
                    String scripCode = getString(doc, "ScripCode");
                    if (scripCode == null || scripCode.isBlank()) continue;

                    int lotSize = parseIntSafe(getString(doc, "LotSize"), 1);
                    int multiplier = parseIntSafe(getString(doc, "Multiplier"), 1);

                    if (lotSize > 0) {
                        cache.put(scripCode, new LotSizeInfo(lotSize, multiplier));
                        count++;
                    }
                } catch (Exception e) {
                    // Skip malformed docs
                }
            }
            log.info("✅ [LOT-SIZE] Loaded {} entries from scripData", count);

            // Also load from ScripGroup for company-specific FUT/Options
            int scripGroupCount = 0;
            for (Document doc : mongoTemplate.getCollection("ScripGroup").find()) {
                try {
                    // Load futures lot sizes
                    List<Document> futures = doc.getList("futures", Document.class);
                    if (futures != null) {
                        for (Document fut : futures) {
                            String sc = getString(fut, "ScripCode");
                            if (sc != null && !cache.containsKey(sc)) {
                                int ls = parseIntSafe(getString(fut, "LotSize"), 1);
                                int mult = parseIntSafe(getString(fut, "Multiplier"), 1);
                                if (ls > 1) {
                                    cache.put(sc, new LotSizeInfo(ls, mult));
                                    scripGroupCount++;
                                }
                            }
                        }
                    }

                    // Load options lot sizes
                    List<Document> options = doc.getList("options", Document.class);
                    if (options != null) {
                        for (Document opt : options) {
                            String sc = getString(opt, "ScripCode");
                            if (sc != null && !cache.containsKey(sc)) {
                                int ls = parseIntSafe(getString(opt, "LotSize"), 1);
                                int mult = parseIntSafe(getString(opt, "Multiplier"), 1);
                                if (ls > 1) {
                                    cache.put(sc, new LotSizeInfo(ls, mult));
                                    scripGroupCount++;
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    // Skip malformed docs
                }
            }
            log.info("✅ [LOT-SIZE] Loaded {} additional entries from ScripGroup. Total cache: {}",
                    scripGroupCount, cache.size());

        } catch (Exception e) {
            log.error("ERR [LOT-SIZE] Failed to preload cache: {}", e.getMessage());
        }
    }

    /**
     * Get lot size for a scripCode. Returns 1 for equity (default).
     */
    public int getLotSize(String scripCode) {
        LotSizeInfo info = cache.get(scripCode);
        if (info == null) {
            log.debug("ERR [LOT-SIZE] scripCode={} not found, using default lotSize=1", scripCode);
            return 1;
        }
        return info.lotSize;
    }

    /**
     * Get multiplier for a scripCode. Returns 1 for equity/NSE (default).
     */
    public int getMultiplier(String scripCode) {
        LotSizeInfo info = cache.get(scripCode);
        if (info == null) {
            return 1;
        }
        return info.multiplier;
    }

    /**
     * Round a raw quantity DOWN to the nearest lot size boundary.
     * Example: rawQty=123, lotSize=30 → returns 120 (4 lots).
     * Returns at least 0 (caller should check and ensure minimum 1 lot).
     */
    public int roundToLotSize(int rawQty, String scripCode) {
        int lotSize = getLotSize(scripCode);
        if (lotSize <= 1) return rawQty; // equity — no rounding needed
        return (rawQty / lotSize) * lotSize;
    }

    private String getString(Document doc, String field) {
        Object val = doc.get(field);
        return val != null ? val.toString() : null;
    }

    private int parseIntSafe(String val, int defaultVal) {
        if (val == null || val.isBlank()) return defaultVal;
        try {
            // Handle values like "30.0" stored as strings
            return (int) Double.parseDouble(val.trim());
        } catch (NumberFormatException e) {
            return defaultVal;
        }
    }

    public static class LotSizeInfo {
        final int lotSize;
        final int multiplier;

        LotSizeInfo(int lotSize, int multiplier) {
            this.lotSize = Math.max(1, lotSize);
            this.multiplier = Math.max(1, multiplier);
        }
    }
}
