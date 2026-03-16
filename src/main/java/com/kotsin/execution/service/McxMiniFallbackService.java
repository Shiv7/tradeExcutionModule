package com.kotsin.execution.service;

import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * MCX Mini/Smaller Lot Fallback Service.
 *
 * When a MCX commodity signal can't be traded because the lot cost exceeds
 * available capital, this service finds a smaller variant of the same commodity
 * (e.g., ALUMINIUM -> ALUMINI, SILVER -> SILVERM -> SILVERMIC).
 *
 * ONLY applies to MCX futures instruments that do NOT have options trading.
 * Commodities with options (CRUDEOIL, NATURALGAS, ZINC, COPPER) should go
 * through OptionDataEnricher's option routing instead.
 */
@Service
@Slf4j
public class McxMiniFallbackService {

    @Autowired
    private MongoTemplate mongoTemplate;

    /**
     * MCX commodities that have active options trading — these should use option routing,
     * NOT the mini fallback mechanism.
     */
    private static final Set<String> MCX_OPTIONS_WHITELIST = Set.of(
        "CRUDEOIL", "NATURALGAS", "COPPER"
    );

    /**
     * Fallback chain: maps a commodity symbol root to an ordered list of smaller variants.
     * The list is ordered from largest to smallest — we try each in order until one is affordable.
     * The first element is always the original symbol itself (for completeness), followed by
     * progressively smaller variants.
     *
     * Example: ALUMINIUM -> [ALUMINI]  (try ALUMINI if ALUMINIUM is too expensive)
     *          SILVER -> [SILVERM, SILVERMIC]  (try SILVERM first, then SILVERMIC)
     *          GOLD -> [GOLDM, GOLDGUINEA, GOLDPETAL, GOLDTEN]
     */
    private static final Map<String, List<String>> FALLBACK_CHAINS;
    static {
        Map<String, List<String>> chains = new HashMap<>();

        // Base metals
        chains.put("ALUMINIUM", List.of("ALUMINI"));
        chains.put("ZINC", List.of("ZINCMINI"));          // Zinc has options, but ZINCMINI does not
        chains.put("LEAD", List.of("LEADMINI"));
        chains.put("NICKEL", List.of("NICKELMINI"));

        // Precious metals — multi-tier fallback
        chains.put("SILVER", List.of("SILVERM", "SILVERMIC"));
        chains.put("SILVERM", List.of("SILVERMIC"));

        chains.put("GOLD", List.of("GOLDM", "GOLDGUINEA", "GOLDPETAL", "GOLDTEN"));
        chains.put("GOLDM", List.of("GOLDGUINEA", "GOLDPETAL", "GOLDTEN"));
        chains.put("GOLDGUINEA", List.of("GOLDPETAL", "GOLDTEN"));
        chains.put("GOLDPETAL", List.of("GOLDTEN"));

        // Energy
        chains.put("CRUDEOIL", List.of("CRUDEOILM"));     // CRUDEOIL has options, but CRUDEOILM does not
        chains.put("NATURALGAS", List.of("NATGASMINI"));   // NATURALGAS has options, but NATGASMINI does not

        FALLBACK_CHAINS = Collections.unmodifiableMap(chains);
    }

    /**
     * Cache: "ALUMINI" -> McxFallbackInfo (scripCode, lotSize, multiplier, name, expiry)
     * Keyed by symbolRoot, stores the nearest-month active contract.
     * Refreshed when expiry passes.
     */
    private final ConcurrentHashMap<String, McxFallbackInfo> fallbackCache = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        log.info("[MCX-FALLBACK] Service initialized. {} fallback chains configured.", FALLBACK_CHAINS.size());
        // Pre-warm cache for all known mini variants
        Set<String> allMiniSymbols = new HashSet<>();
        for (List<String> chain : FALLBACK_CHAINS.values()) {
            allMiniSymbols.addAll(chain);
        }
        for (String symbol : allMiniSymbols) {
            lookupNearestContract(symbol);
        }
        log.info("[MCX-FALLBACK] Pre-warmed cache with {} mini variant lookups. Cache size: {}",
                allMiniSymbols.size(), fallbackCache.size());
    }

    /**
     * Attempt to find a smaller MCX variant that is affordable.
     *
     * @param symbolRoot      The original commodity symbol root (e.g., "ALUMINIUM")
     * @param exchange        Exchange code (must be "M" for MCX)
     * @param capitalPerTrade Available capital for this trade
     * @param spotPrice       Current spot/underlying price
     * @return McxFallbackResult with the mini variant details, or null if no affordable variant found
     */
    public McxFallbackResult findAffordableMiniVariant(
            String symbolRoot, String exchange, double capitalPerTrade, double spotPrice) {

        // Only applies to MCX
        if (!"M".equalsIgnoreCase(exchange)) {
            return null;
        }

        // Don't apply mini fallback to options-whitelist commodities
        // (they should go through option routing instead)
        // EXCEPTION: We DO allow fallback for mini variants OF whitelist commodities
        // (e.g., CRUDEOILM is a valid fallback for CRUDEOIL when options aren't available)
        if (MCX_OPTIONS_WHITELIST.contains(symbolRoot)) {
            log.debug("[MCX-FALLBACK] {} is on options whitelist, skipping mini fallback", symbolRoot);
            return null;
        }

        // Get the fallback chain for this symbol
        List<String> fallbackChain = FALLBACK_CHAINS.get(symbolRoot);
        if (fallbackChain == null || fallbackChain.isEmpty()) {
            log.debug("[MCX-FALLBACK] No fallback chain for {}", symbolRoot);
            return null;
        }

        // Try each smaller variant in order
        for (String miniSymbol : fallbackChain) {
            McxFallbackInfo info = lookupNearestContract(miniSymbol);
            if (info == null) {
                log.debug("[MCX-FALLBACK] No active contract found for {}", miniSymbol);
                continue;
            }

            // Calculate lot cost for this variant
            // For futures, cost = price * multiplier (multiplier includes lot size effect)
            double miniLotCost = spotPrice * info.multiplier;

            if (miniLotCost <= capitalPerTrade && miniLotCost > 0) {
                int lots = (int) Math.floor(capitalPerTrade / miniLotCost);
                log.info("MCX_MINI_FALLBACK: {} -> {} (lot cost {} -> {}, can afford {} lots with capital {})",
                        symbolRoot, miniSymbol,
                        String.format("%.0f", spotPrice * getOriginalMultiplier(symbolRoot)),
                        String.format("%.0f", miniLotCost),
                        lots, String.format("%.0f", capitalPerTrade));

                return new McxFallbackResult(
                        info.scripCode,
                        miniSymbol,
                        info.name,
                        info.lotSize,
                        info.multiplier,
                        miniLotCost,
                        info.expiry,
                        lots
                );
            } else {
                log.debug("[MCX-FALLBACK] {} still too expensive: lotCost={} > capital={}",
                        miniSymbol, String.format("%.0f", miniLotCost), String.format("%.0f", capitalPerTrade));
            }
        }

        log.info("[MCX-FALLBACK] No affordable variant found for {} (capital={})",
                symbolRoot, String.format("%.0f", capitalPerTrade));
        return null;
    }

    /**
     * Find the cheapest variant's lot cost for a given commodity symbol root.
     * Iterates the ENTIRE fallback chain and returns the smallest lot cost
     * (last/smallest variant). Used by computeMinLotCost() to report the minimum
     * possible cost so FundAllocationService doesn't over-reject.
     *
     * @param symbolRoot The original commodity symbol root (e.g., "SILVER")
     * @param spotPrice  Current spot/underlying price
     * @return The cheapest variant's lot cost (price * multiplier), or 0 if no variants found
     */
    public double findCheapestVariantCost(String symbolRoot, double spotPrice) {
        List<String> fallbackChain = FALLBACK_CHAINS.get(symbolRoot);
        if (fallbackChain == null || fallbackChain.isEmpty()) return 0;

        double cheapest = Double.MAX_VALUE;
        for (String miniSymbol : fallbackChain) {
            McxFallbackInfo info = lookupNearestContract(miniSymbol);
            if (info == null) continue;
            double cost = spotPrice * info.multiplier;
            if (cost > 0 && cost < cheapest) {
                cheapest = cost;
            }
        }
        return cheapest < Double.MAX_VALUE ? cheapest : 0;
    }

    /**
     * Extract the symbol root from a companyName like "ALUMINIUM 31 MAR 2026".
     */
    public static String extractSymbolRoot(String companyName) {
        if (companyName == null || companyName.isBlank()) return null;
        // Split on first space — symbol root is the first token
        String[] parts = companyName.trim().split("\\s+", 2);
        return parts[0].toUpperCase();
    }

    /**
     * Check if a symbol root has a fallback chain (i.e., has smaller variants).
     */
    public boolean hasFallbackChain(String symbolRoot) {
        return symbolRoot != null && FALLBACK_CHAINS.containsKey(symbolRoot);
    }

    /**
     * Check if a symbol root is on the MCX options whitelist.
     */
    public boolean isOptionsWhitelisted(String symbolRoot) {
        return symbolRoot != null && MCX_OPTIONS_WHITELIST.contains(symbolRoot);
    }

    // ==================== MongoDB Lookup ====================

    /**
     * Look up the nearest-month active contract for a given MCX symbol root.
     * Uses cache with expiry-aware invalidation.
     */
    private McxFallbackInfo lookupNearestContract(String symbolRoot) {
        // Check cache first
        McxFallbackInfo cached = fallbackCache.get(symbolRoot);
        if (cached != null && !cached.isExpired()) {
            return cached;
        }

        try {
            // Query scripData for the nearest active contract:
            // - SymbolRoot matches
            // - Exch = "M" (MCX)
            // - Expiry >= today
            // - Sort by Expiry ascending (nearest first)
            String today = LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE);

            Document query = new Document()
                    .append("SymbolRoot", symbolRoot)
                    .append("Exch", "M")
                    .append("Expiry", new Document("$gte", today));

            Document sort = new Document("Expiry", 1); // nearest expiry first

            Document doc = mongoTemplate.getCollection("scripData")
                    .find(query)
                    .sort(sort)
                    .first();

            if (doc == null) {
                log.warn("[MCX-FALLBACK] No active contract found in scripData for SymbolRoot={}", symbolRoot);
                return null;
            }

            String scripCode = getString(doc, "ScripCode");
            String name = getString(doc, "Name");
            int lotSize = parseIntSafe(getString(doc, "LotSize"), 1);
            int multiplier = parseIntSafe(getString(doc, "Multiplier"), 1);
            String expiry = getString(doc, "Expiry");

            McxFallbackInfo info = new McxFallbackInfo(scripCode, symbolRoot, name, lotSize, multiplier, expiry);
            fallbackCache.put(symbolRoot, info);

            log.info("[MCX-FALLBACK] Resolved {}  scripCode={} name={} lotSize={} multiplier={} expiry={}",
                    symbolRoot, scripCode, name, lotSize, multiplier, expiry);
            return info;

        } catch (Exception e) {
            log.error("[MCX-FALLBACK] MongoDB lookup failed for {}: {}", symbolRoot, e.getMessage());
            return null;
        }
    }

    /**
     * Get the original multiplier for a symbol root (for logging the original lot cost).
     * Falls back to looking up from cache or returning 1.
     */
    private int getOriginalMultiplier(String symbolRoot) {
        McxFallbackInfo info = fallbackCache.get(symbolRoot);
        if (info != null) return info.multiplier;
        // Try looking up the original symbol
        McxFallbackInfo original = lookupNearestContract(symbolRoot);
        return original != null ? original.multiplier : 1;
    }

    private String getString(Document doc, String field) {
        Object val = doc.get(field);
        return val != null ? val.toString() : null;
    }

    private int parseIntSafe(String val, int defaultVal) {
        if (val == null || val.isBlank()) return defaultVal;
        try {
            return (int) Double.parseDouble(val.trim());
        } catch (NumberFormatException e) {
            return defaultVal;
        }
    }

    // ==================== Data Classes ====================

    /**
     * Cached info about a MCX mini/smaller variant contract.
     */
    public static class McxFallbackInfo {
        public final String scripCode;
        public final String symbolRoot;
        public final String name;
        public final int lotSize;
        public final int multiplier;
        public final String expiry;

        McxFallbackInfo(String scripCode, String symbolRoot, String name,
                        int lotSize, int multiplier, String expiry) {
            this.scripCode = scripCode;
            this.symbolRoot = symbolRoot;
            this.name = name;
            this.lotSize = lotSize;
            this.multiplier = multiplier;
            this.expiry = expiry;
        }

        /**
         * Check if this contract has expired (expiry date has passed).
         */
        boolean isExpired() {
            if (expiry == null) return true;
            try {
                LocalDate expiryDate = LocalDate.parse(expiry, DateTimeFormatter.ISO_LOCAL_DATE);
                return LocalDate.now().isAfter(expiryDate);
            } catch (Exception e) {
                return false; // If parsing fails, assume still valid
            }
        }
    }

    /**
     * Result of a successful mini fallback lookup — contains everything needed
     * to substitute the mini variant into the trade.
     */
    public static class McxFallbackResult {
        public final String scripCode;
        public final String symbolRoot;
        public final String name;
        public final int lotSize;
        public final int multiplier;
        public final double lotCost;
        public final String expiry;
        public final int affordableLots;

        McxFallbackResult(String scripCode, String symbolRoot, String name,
                          int lotSize, int multiplier, double lotCost,
                          String expiry, int affordableLots) {
            this.scripCode = scripCode;
            this.symbolRoot = symbolRoot;
            this.name = name;
            this.lotSize = lotSize;
            this.multiplier = multiplier;
            this.lotCost = lotCost;
            this.expiry = expiry;
            this.affordableLots = affordableLots;
        }
    }
}
