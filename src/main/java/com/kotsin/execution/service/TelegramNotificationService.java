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

    @Value("${telegram.chat.id:-4640817596}")
    private String chatId;

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
            String message = formatEntryMessage(trade);
            return sendMessage(message);
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
            String message = formatExitMessage(trade, result);
            return sendMessage(message);
        } catch (Exception e) {
            log.error("🚨 Error sending Telegram trade exit notification: {}", e.getMessage(), e);
            messagesFailed.incrementAndGet();
            return false;
        }
    }

    private String formatEntryMessage(ActiveTrade trade) {
        long alertNumber = messagesSent.incrementAndGet();
        String currentTime = LocalDateTime.now(ZoneId.of("Asia/Kolkata")).format(TIME_FORMATTER);

        return String.format(
            "<b>✅ Trade Entry #%d ✅</b>\n\n" +
            "<b>%s %s</b>\n" +
            "<b>%s (%s)</b>\n" +
            "⏰ %s\n" +
            "💰 Price: ₹%.2f\n" +
            "📊 Quantity: %d\n" +
            "📈 Strategy: %s\n" +
            "🛑 Stoploss: ₹%.2f",
            alertNumber, getActionEmoji(trade.getSignalType()), trade.getSignalType(),
            trade.getCompanyName(), trade.getScripCode(),
            currentTime,
            trade.getEntryPrice(),
            trade.getPositionSize(),
            trade.getStrategyName(),
            trade.getStopLoss()
        );
    }

    private String formatExitMessage(ActiveTrade trade, TradeResult result) {
        long alertNumber = messagesSent.incrementAndGet();
        String currentTime = LocalDateTime.now(ZoneId.of("Asia/Kolkata")).format(TIME_FORMATTER);

        return String.format(
            "<b>❌ Trade Exit #%d ❌</b>\n\n" +
            "<b>%s %s</b>\n" +
            "<b>%s (%s)</b>\n" +
            "⏰ %s\n" +
            "💰 Exit Price: ₹%.2f\n" +
            "Reason: %s\n" +
            "PnL: ₹%.2f",
            alertNumber, getActionEmoji(trade.getSignalType()), trade.getSignalType(),
            trade.getCompanyName(), trade.getScripCode(),
            currentTime,
            trade.getExitPrice(),
            trade.getExitReason(),
            result.getPnL()
        );
    }

    private boolean sendMessage(String message) {
        // This method is copied directly from the strategyModule's service
        // ... (implementation is identical)
        HttpResponse<String> response = null;
        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        try {
            String encodedMessage = URLEncoder.encode(message, StandardCharsets.UTF_8);
            String uriString = String.format(
                "https://api.telegram.org/bot%s/sendMessage?chat_id=%s&text=%s&parse_mode=html",
                telegramToken, chatId, encodedMessage
            );
            HttpRequest request = HttpRequest.newBuilder().GET().uri(URI.create(uriString)).timeout(Duration.ofSeconds(10)).build();
            response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                messagesSuccessful.incrementAndGet();
                log.info("✅ Telegram message sent successfully.");
                return true;
            } else {
                messagesFailed.incrementAndGet();
                log.warn("⚠️ Telegram API returned non-200 status: {}. Response: {}", response.statusCode(), response.body());
                return false;
            }
        } catch (IOException | InterruptedException ex) {
            messagesFailed.incrementAndGet();
            log.error("🚨 Failed to send Telegram message: {}", ex.getMessage(), ex);
            return false;
        }
    }

    private String getActionEmoji(String signal) {
        if ("BUY".equalsIgnoreCase(signal)) return "🟢";
        if ("SELL".equalsIgnoreCase(signal)) return "🔴";
        return "⚪";
    }
} 