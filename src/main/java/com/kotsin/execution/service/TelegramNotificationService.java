package com.kotsin.execution.service;

import com.kotsin.execution.model.ActiveTrade;
import com.kotsin.execution.model.TradeResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicLong;

@Service
@Slf4j
public class TelegramNotificationService {

    @Value("${telegram.bot.token:6110276523:AAFNH9wRYkQQymniK8ioE0EnmN_6pfUZkJk}")
    private String telegramToken;

    @Value("${telegram.execution.chat.id:-4987106706}")
    private String executionChatId;
    
    @Value("${telegram.pnl.chat.id:-4924122957}")
    private String pnlChatId;

    @Value("${telegram.enabled:true}")
    private boolean telegramEnabled;

    // Metrics
    private final AtomicLong messagesSent = new AtomicLong(0);
    private final AtomicLong messagesSuccessful = new AtomicLong(0);
    private final AtomicLong messagesFailed = new AtomicLong(0);

    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

    /**
     * Send a formatted TRADE ENTRY notification.
     */
    public boolean sendTradeNotification(ActiveTrade trade) {
        if (!telegramEnabled) {
            log.debug("📱 Telegram notifications disabled, skipping trade notification.");
            return false;
        }
        if (trade == null) {
            log.warn("❌ Cannot send Telegram alert: null trade object");
            return false;
        }
        try {
            log.info("📱 [TRADE-EXECUTION] Sending trade entry to EXECUTION channel: {}", executionChatId);
            String message = formatEntryMessage(trade);
            return sendMessage(message, executionChatId);
        } catch (Exception e) {
            log.error("🚨 Error sending Telegram trade entry notification: {}", e.getMessage(), e);
            messagesFailed.incrementAndGet();
            return false;
        }
    }

    /**
     * Send a formatted TRADE EXIT notification.
     */
    public boolean sendTradeNotification(ActiveTrade trade, TradeResult result) {
        if (!telegramEnabled) {
            log.debug("📱 Telegram notifications disabled, skipping trade notification.");
            return false;
        }
        if (trade == null || result == null) {
            log.warn("❌ Cannot send Telegram alert: null trade or result object");
            return false;
        }
        try {
            log.info("📱 [TRADE-EXECUTION] Sending P&L to PnL channel: {}", pnlChatId);
            String message = formatExitMessage(trade, result);
            return sendMessage(message, pnlChatId);
        } catch (Exception e) {
            log.error("🚨 Error sending Telegram trade exit notification: {}", e.getMessage(), e);
            messagesFailed.incrementAndGet();
            return false;
        }
    }

    /**
     * Send a detailed TIMEOUT notification for delayed entry trades
     */
    public boolean sendTimeoutNotification(String message) {
        if (!telegramEnabled) {
            log.debug("📱 Telegram notifications disabled, skipping timeout notification.");
            return false;
        }
        if (message == null || message.trim().isEmpty()) {
            log.warn("❌ Cannot send Telegram timeout alert: empty message");
            return false;
        }
        try {
            log.info("📱 [TRADE-TIMEOUT] Sending timeout notification to PnL channel: {}", pnlChatId);
            return sendMessage(message, pnlChatId);
        } catch (Exception e) {
            log.error("🚨 Error sending Telegram timeout notification: {}", e.getMessage(), e);
            messagesFailed.incrementAndGet();
            return false;
        }
    }

    /**
     * 🛡️ BULLETPROOF: Send general trade notification message to execution channel
     */
    public boolean sendTradeNotificationMessage(String message) {
        if (!telegramEnabled) {
            log.debug("📱 Telegram notifications disabled, skipping trade notification.");
            return false;
        }
        if (message == null || message.trim().isEmpty()) {
            log.warn("❌ Cannot send Telegram alert: empty message");
            return false;
        }
        try {
            log.info("📱 [BULLETPROOF-TRADE] Sending trade notification to EXECUTION channel: {}", executionChatId);
            return sendMessage(message, executionChatId);
        } catch (Exception e) {
            log.error("🚨 Error sending Telegram trade notification: {}", e.getMessage(), e);
            messagesFailed.incrementAndGet();
            return false;
        }
    }

    private String formatEntryMessage(ActiveTrade trade) {
        long alertNumber = messagesSent.incrementAndGet();
        
        // Enhanced time formatting
        DateTimeFormatter fullTimeFormatter = DateTimeFormatter.ofPattern("MMM dd, HH:mm:ss");
        String entryTime = LocalDateTime.now(ZoneId.of("Asia/Kolkata")).format(fullTimeFormatter);
        
        // Strategy context
        String strategyInfo = trade.getStrategyName() != null ? 
            trade.getStrategyName() : "Enhanced 30M Strategy";
        
        // Risk-Reward context
        String rrContext = "";
        Double rrRatio = trade.getRiskRewardRatio();
        if (rrRatio != null && rrRatio > 0) {
            rrContext = String.format("\n📊 R:R Ratio: 1:%.2f", rrRatio);
        }
        
        // Target levels information
        String targetLevels = "";
        if (trade.getTarget1() != null) {
            targetLevels += String.format("\n🎯 Target 1: ₹%.2f", trade.getTarget1());
            if (trade.getTarget2() != null && trade.getTarget2() > 0) {
                targetLevels += String.format(" | T2: ₹%.2f", trade.getTarget2());
            }
        }

        return String.format(
            "<b>🟢 TRADE ENTRY #%d 🟢</b>\n\n" +
            "%s <b>%s for %s</b>\n" +
            "<b>%s (%s)</b>\n\n" +
            "📈 <b>TRADE SETUP:</b>\n" +
            "🟢 Entry: %s at ₹%.2f\n" +
            "🛑 Stop Loss: ₹%.2f%s%s\n\n" +
            "💼 <b>STRATEGY:</b> %s\n" +
            "📊 Position Size: %d shares\n" +
            "💰 Capital Risk: ₹%.2f",
            
            alertNumber,
            getActionEmoji(trade.getSignalType()), 
            cleanText(trade.getSignalType()), 
            cleanText(trade.getCompanyName()),
            cleanText(trade.getCompanyName()), 
            cleanText(trade.getScripCode()),
            entryTime, trade.getEntryPrice(),
            trade.getStopLoss(), targetLevels, rrContext,
            cleanText(strategyInfo),
            trade.getPositionSize() != null ? trade.getPositionSize() : 0,
            calculateRiskAmount(trade)
        );
    }
    
    /**
     * Calculate the capital at risk for the trade
     */
    private double calculateRiskAmount(ActiveTrade trade) {
        if (trade.getEntryPrice() == null || trade.getStopLoss() == null || trade.getPositionSize() == null) {
            return 0.0;
        }
        
        double riskPerShare = Math.abs(trade.getEntryPrice() - trade.getStopLoss());
        return riskPerShare * trade.getPositionSize();
    }

    private String formatExitMessage(ActiveTrade trade, TradeResult result) {
        long alertNumber = messagesSent.incrementAndGet();
        
        // Enhanced time formatting for both entry and exit
        DateTimeFormatter fullTimeFormatter = DateTimeFormatter.ofPattern("MMM dd, HH:mm:ss");
        DateTimeFormatter timeOnlyFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");
        
        String entryTime = trade.getEntryTime() != null ? 
            trade.getEntryTime().format(fullTimeFormatter) : "N/A";
        String exitTime = LocalDateTime.now(ZoneId.of("Asia/Kolkata")).format(timeOnlyFormatter);
        
        // Calculate trade duration
        String duration = "N/A";
        if (trade.getEntryTime() != null) {
            long minutes = java.time.Duration.between(trade.getEntryTime(), LocalDateTime.now()).toMinutes();
            if (minutes < 60) {
                duration = minutes + " min";
            } else {
                long hours = minutes / 60;
                long remainingMinutes = minutes % 60;
                duration = hours + "h " + remainingMinutes + "m";
            }
        }
        
        // Enhanced PnL formatting with percentage
        double pnl = result.getPnL() != null ? result.getPnL() : 0.0;
        String pnlEmoji = pnl >= 0 ? "💰" : "💸";
        String pnlStatus = pnl >= 0 ? "PROFIT" : "LOSS";
        
        // ROI calculation
        String roiText = "";
        if (result.getRoi() != null) {
            roiText = String.format(" (%.2f%%)", result.getRoi());
        }
        
        // Risk-Reward context
        String rrContext = "";
        Double rrRatio = trade.getRiskRewardRatio();
        if (rrRatio != null && rrRatio > 0) {
            rrContext = String.format("\n📊 R:R Ratio: 1:%.2f", rrRatio);
        }
        
        // Target hit information
        String targetInfo = "";
        if (trade.getTarget1Hit() != null && trade.getTarget1Hit()) {
            targetInfo += "\n🎯 Target 1: HIT";
            if (trade.getTarget2Hit() != null && trade.getTarget2Hit()) {
                targetInfo += " | Target 2: HIT";
            }
        } else {
            targetInfo += "\n🎯 Targets: MISSED";
        }
        
        // Strategy context
        String strategyInfo = trade.getStrategyName() != null ? 
            trade.getStrategyName() : "Enhanced 30M Strategy";
        
        // Enhanced exit reason
        String exitReason = trade.getExitReason() != null ? trade.getExitReason() : "Unknown";
        String exitReasonEmoji = getExitReasonEmoji(exitReason);
        
        return String.format(
            "<b>%s %s Trade Exit #%d %s</b>\n\n" +
            "%s <b>%s for %s</b>\n" +
            "<b>%s (%s)</b>\n\n" +
            "📈 <b>TRADE TIMELINE:</b>\n" +
            "🟢 Entry: %s at ₹%.2f\n" +
            "🔴 Exit: %s at ₹%.2f\n" +
            "⏱️ Duration: %s\n\n" +
            "💼 <b>STRATEGY:</b> %s\n" +
            "%s Exit Reason: %s\n%s%s\n\n" +
            "%s <b>P&L: ₹%.2f%s</b>\n" +
            "📊 Position Size: %d shares",
            
            pnlEmoji, pnlStatus, alertNumber, pnlEmoji,
            getActionEmoji(trade.getSignalType()), 
            cleanText(trade.getSignalType()), 
            cleanText(trade.getCompanyName()),
            cleanText(trade.getCompanyName()), 
            cleanText(trade.getScripCode()),
            entryTime, trade.getEntryPrice(),
            exitTime, trade.getExitPrice(),
            duration,
            cleanText(strategyInfo),
            exitReasonEmoji, cleanText(exitReason), rrContext, targetInfo,
            pnlEmoji, pnl, roiText,
            trade.getPositionSize() != null ? trade.getPositionSize() : 0
        );
    }
    
    /**
     * Get appropriate emoji for exit reason
     */
    private String getExitReasonEmoji(String exitReason) {
        if (exitReason == null) return "❓";
        
        String reason = exitReason.toUpperCase();
        if (reason.contains("TARGET")) return "🎯";
        if (reason.contains("STOP") || reason.contains("LOSS")) return "🛑";
        if (reason.contains("TIME") || reason.contains("TIMEOUT")) return "⏰";
        if (reason.contains("TRAILING")) return "📈";
        if (reason.contains("MANUAL")) return "👤";
        
        return "❓";
    }

    private boolean sendMessage(String message, String chatId) {
        HttpResponse<String> response = null;
        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        try {
            String cleanedMessage = cleanMessage(message);
            String encodedMessage = URLEncoder.encode(cleanedMessage, StandardCharsets.UTF_8);
            String uriString = String.format(
                "https://api.telegram.org/bot%s/sendMessage?chat_id=%s&text=%s&parse_mode=html",
                telegramToken, chatId, encodedMessage
            );
            HttpRequest request = HttpRequest.newBuilder().GET().uri(URI.create(uriString)).timeout(Duration.ofSeconds(10)).build();
            response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                messagesSuccessful.incrementAndGet();
                log.info("✅ Telegram message sent successfully to channel: {}", chatId);
                return true;
            } else {
                messagesFailed.incrementAndGet();
                log.warn("⚠️ Telegram API returned non-200 status: {}. Response: {}", response.statusCode(), response.body());
                
                if (response.body().contains("can't parse entities") || response.body().contains("Bad Request")) {
                    log.info("🔄 Retrying with plain text format due to HTML parsing error");
                    return sendPlainTextMessage(message, chatId);
                }
                
                return false;
            }
        } catch (IOException | InterruptedException ex) {
            messagesFailed.incrementAndGet();
            log.error("🚨 Failed to send Telegram message: {}", ex.getMessage(), ex);
            return false;
        }
    }

    private String getActionEmoji(String signal) {
        if ("BUY".equalsIgnoreCase(signal) || "BULLISH".equalsIgnoreCase(signal)) return "🟢";
        if ("SELL".equalsIgnoreCase(signal) || "BEARISH".equalsIgnoreCase(signal)) return "🔴";
        return "⚪";
    }

    /**
     * Clean text to prevent HTML parsing issues
     */
    private String cleanText(String text) {
        if (text == null) return "";
        
        // Remove or escape problematic characters
        return text.replace("<", "&lt;")
                  .replace(">", "&gt;")
                  .replace("&", "&amp;")
                  .trim();
    }
    
    /**
     * Clean entire message to prevent parsing errors
     */
    private String cleanMessage(String message) {
        if (message == null) return "";
        
        // Remove any problematic empty tags or malformed HTML
        return message.replaceAll("<[^>]*>(?=\\s*<)", "") // Remove empty tags
                     .replaceAll("\\s+", " ") // Normalize whitespace
                     .trim();
    }
    
    /**
     * Fallback method to send plain text message if HTML fails
     */
    private boolean sendPlainTextMessage(String message, String chatId) {
        try {
            // Strip all HTML tags for plain text
            String plainMessage = message.replaceAll("<[^>]*>", "");
            String encodedMessage = URLEncoder.encode(plainMessage, StandardCharsets.UTF_8);
            
            String uriString = String.format(
                "https://api.telegram.org/bot%s/sendMessage?chat_id=%s&text=%s",
                telegramToken, chatId, encodedMessage
            );
            
            HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
            HttpRequest request = HttpRequest.newBuilder().GET().uri(URI.create(uriString)).timeout(Duration.ofSeconds(10)).build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                messagesSuccessful.incrementAndGet();
                log.info("✅ Telegram plain text message sent successfully to channel: {}", chatId);
                return true;
            } else {
                log.warn("⚠️ Telegram plain text also failed. Status: {}, Response: {}", response.statusCode(), response.body());
                return false;
            }
        } catch (Exception ex) {
            log.error("🚨 Failed to send plain text Telegram message: {}", ex.getMessage(), ex);
            return false;
        }
    }
} 