package com.kotsin.execution.wallet.service;

import com.kotsin.execution.virtual.model.VirtualOrder;
import com.kotsin.execution.virtual.model.VirtualPosition;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Resolves which strategy wallet to use for a given order or position.
 *
 * Routing: VirtualOrder.signalSource → resolveStrategyKey() → walletIdForStrategy()
 * Fallback: orders without signalSource route to the legacy shared wallet (defaultWalletId).
 */
@Component
@Slf4j
public class StrategyWalletResolver {

    // STRATEGY-WALLET-SOP: Add new strategy keys here. Wallet auto-creates on startup.
    public static final List<String> ALL_STRATEGY_KEYS = List.of(
            "FUDKII", "FUKAA", "FUDKOI", "PIVOT_CONFLUENCE", "MICROALPHA", "MERE", "QUANT", "MCX_BB", "MCX_BBT1"
    );

    @Value("${wallet.id:virtual-wallet-1}")
    private String defaultWalletId;

    /**
     * Map signalSource (and optionally signalType) to a canonical strategy key.
     * Returns null if no mapping found (will fall back to default wallet).
     */
    public static String resolveStrategyKey(String signalSource, String signalType) {
        if (signalSource == null || signalSource.isBlank()) {
            return null;
        }

        String src = signalSource.trim().toUpperCase();

        // Direct match
        if (ALL_STRATEGY_KEYS.contains(src)) {
            return src;
        }

        // Alias mappings
        return switch (src) {
            case "PIVOT", "PIVOT_CONFLUENCE" -> "PIVOT_CONFLUENCE";
            case "MICRO", "MICROALPHA", "MICRO_ALPHA" -> "MICROALPHA";
            case "QUANT", "QUANT_HEDGE" -> "QUANT"; // BUG-004 FIX: QUANT gets its own wallet
            case "MERE_SCALP", "MERE_SWING", "MERE_POSITIONAL" -> "MERE";
            case "MCX_BB", "MCXBB", "MCX-BB" -> "MCX_BB";
            case "MCX_BBT1", "MCXBBT1", "MCX-BBT1", "MCX_BBT+1" -> "MCX_BBT1";
            case "MANUAL" -> null; // Manual orders → default wallet
            default -> {
                log.warn("ERR [WALLET-RESOLVE] Unknown signalSource={}, falling back to default wallet", src);
                yield null;
            }
        };
    }

    /**
     * Build wallet ID from strategy key.
     */
    public static String walletIdForStrategy(String strategyKey) {
        return "strategy-wallet-" + strategyKey;
    }

    /**
     * Resolve the wallet ID for an order.
     * Falls back to defaultWalletId if strategy cannot be determined.
     */
    public String resolveWalletId(VirtualOrder order) {
        String key = resolveStrategyKey(order.getSignalSource(), order.getSignalType());
        if (key == null) {
            return defaultWalletId;
        }
        return walletIdForStrategy(key);
    }

    /**
     * Resolve the wallet ID for a position (used on close/P&L credit).
     * Falls back to defaultWalletId if strategy cannot be determined.
     */
    public String resolveWalletIdFromPosition(VirtualPosition position) {
        String key = resolveStrategyKey(position.getSignalSource(), position.getSignalType());
        if (key == null) {
            return defaultWalletId;
        }
        return walletIdForStrategy(key);
    }

    /**
     * Extract strategy key from a wallet ID.
     * Returns null if the walletId is not a strategy wallet.
     */
    public static String strategyKeyFromWalletId(String walletId) {
        if (walletId != null && walletId.startsWith("strategy-wallet-")) {
            return walletId.substring("strategy-wallet-".length());
        }
        return null;
    }

    public String getDefaultWalletId() {
        return defaultWalletId;
    }
}
