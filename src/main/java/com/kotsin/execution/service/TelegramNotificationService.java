package com.kotsin.execution.service;

import com.kotsin.execution.model.ActiveTrade;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import okhttp3.FormBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class TelegramNotificationService {

    private final OkHttpClient http = new OkHttpClient();

    @Value("${telegram.bot.token:}")
    private String botToken;

    @Value("${telegram.chat.id:}")
    private String chatId;

    public void sendTradeNotification(ActiveTrade trade) {
        if (botToken == null || botToken.isBlank() || chatId == null || chatId.isBlank()) {
            log.debug("Telegram not configured; skipping message.");
            return;
        }
        String text = String.format(
                "TRADE ENTERED\\n%s (%s)\\nEntry: %.2f SL: %.2f T1: %.2f",
                trade.getCompanyName(), trade.getScripCode(),
                trade.getEntryPrice(), trade.getStopLoss(), trade.getTarget1()
        );
        String url = "https://api.telegram.org/bot" + botToken + "/sendMessage";
        FormBody body = new FormBody.Builder()
                .add("chat_id", chatId)
                .add("text", text)
                .build();
        Request req = new Request.Builder().url(url).post(body).build();
        try (Response resp = http.newCall(req).execute()) {
            if (!resp.isSuccessful()) {
                log.warn("Telegram send failed: HTTP {}", resp.code());
            }
        } catch (Exception e) {
            log.warn("Telegram send error: {}", e.toString());
        }
    }
}
