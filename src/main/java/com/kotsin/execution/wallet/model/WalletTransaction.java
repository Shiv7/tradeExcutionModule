package com.kotsin.execution.wallet.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Represents a transaction affecting the wallet balance.
 * Provides audit trail for all wallet operations.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WalletTransaction {

    private String transactionId;
    private String walletId;
    private TransactionType type;

    // Order/Position Reference
    private String orderId;
    private String positionId;
    private String scripCode;
    private String symbol;

    // Transaction Details
    private double amount;           // Positive for credit, negative for debit
    private double balanceBefore;
    private double balanceAfter;
    private double marginBefore;
    private double marginAfter;

    // Trade Details (for MARGIN_DEDUCT / PNL_CREDIT)
    private String side;             // BUY or SELL
    private int quantity;
    private double entryPrice;
    private double exitPrice;
    private double pnl;

    // Metadata
    private String description;
    private String reason;           // EXIT_REASON for PNL transactions
    private LocalDateTime timestamp;

    public enum TransactionType {
        DEPOSIT,           // Initial deposit or add funds
        WITHDRAWAL,        // Withdraw funds
        MARGIN_RESERVE,    // Reserve margin for pending order
        MARGIN_RELEASE,    // Release reserved margin (order cancelled)
        MARGIN_DEDUCT,     // Deduct margin when order fills
        MARGIN_RETURN,     // Return margin when position closes
        PNL_CREDIT,        // Credit realized P&L (can be negative for loss)
        FEE_DEDUCT,        // Deduct trading fees/commissions
        ADJUSTMENT,        // Manual adjustment
        DAILY_RESET        // Daily P&L reset marker
    }

    /**
     * Create margin deduction transaction for order fill
     */
    public static WalletTransaction marginDeduct(
            String walletId, String orderId, String scripCode, String symbol,
            String side, int qty, double price, double balanceBefore, double marginBefore) {

        double marginAmount = price * qty;
        return WalletTransaction.builder()
                .transactionId(generateId())
                .walletId(walletId)
                .type(TransactionType.MARGIN_DEDUCT)
                .orderId(orderId)
                .scripCode(scripCode)
                .symbol(symbol)
                .side(side)
                .quantity(qty)
                .entryPrice(price)
                .amount(-marginAmount)
                .balanceBefore(balanceBefore)
                .balanceAfter(balanceBefore)  // Balance doesn't change, margin does
                .marginBefore(marginBefore)
                .marginAfter(marginBefore + marginAmount)
                .description(String.format("Margin blocked for %s %d %s @ %.2f", side, qty, scripCode, price))
                .timestamp(LocalDateTime.now())
                .build();
    }

    /**
     * Create P&L credit transaction for position close
     */
    public static WalletTransaction pnlCredit(
            String walletId, String positionId, String scripCode, String symbol,
            String side, int qty, double entryPrice, double exitPrice,
            double pnl, String exitReason, double balanceBefore, double marginBefore) {

        double marginReleased = entryPrice * qty;
        return WalletTransaction.builder()
                .transactionId(generateId())
                .walletId(walletId)
                .type(TransactionType.PNL_CREDIT)
                .positionId(positionId)
                .scripCode(scripCode)
                .symbol(symbol)
                .side(side)
                .quantity(qty)
                .entryPrice(entryPrice)
                .exitPrice(exitPrice)
                .pnl(pnl)
                .amount(pnl)
                .balanceBefore(balanceBefore)
                .balanceAfter(balanceBefore + pnl)
                .marginBefore(marginBefore)
                .marginAfter(marginBefore - marginReleased)
                .reason(exitReason)
                .description(String.format("P&L %s%.2f for %s %d %s (exit: %s)",
                        pnl >= 0 ? "+" : "", pnl, side, qty, scripCode, exitReason))
                .timestamp(LocalDateTime.now())
                .build();
    }

    /**
     * Create margin reserve transaction for pending order
     */
    public static WalletTransaction marginReserve(
            String walletId, String orderId, String scripCode,
            double marginAmount, double balanceBefore, double reservedBefore) {

        return WalletTransaction.builder()
                .transactionId(generateId())
                .walletId(walletId)
                .type(TransactionType.MARGIN_RESERVE)
                .orderId(orderId)
                .scripCode(scripCode)
                .amount(-marginAmount)
                .balanceBefore(balanceBefore)
                .balanceAfter(balanceBefore)
                .marginBefore(reservedBefore)
                .marginAfter(reservedBefore + marginAmount)
                .description(String.format("Reserved %.2f margin for pending order %s", marginAmount, orderId))
                .timestamp(LocalDateTime.now())
                .build();
    }

    private static String generateId() {
        return "TXN_" + System.currentTimeMillis() + "_" +
                String.format("%04d", (int) (Math.random() * 10000));
    }
}
