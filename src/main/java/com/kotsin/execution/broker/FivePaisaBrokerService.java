package com.kotsin.execution.broker;

import com.FivePaisa.config.AppConfig;
import com.FivePaisa.api.RestClient;
import com.FivePaisa.service.Properties;
import lombok.extern.slf4j.Slf4j;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.util.Collections;
import java.util.Map;

/**
 * Thin wrapper around 5Paisa RestClient jar – converts our domain calls to broker REST payloads.
 */
@Service
@Slf4j
public class FivePaisaBrokerService implements BrokerOrderService {

    private final AppConfig appConfig = new AppConfig();
    private RestClient restClient;

    // Injected from application.properties
    @Value("${fivepaisa.login-id}")
    private String loginId;
    @Value("${fivepaisa.totp}")
    private String totp;
    @Value("${fivepaisa.pin}")
    private String pin;

    // If fivepaisa.totp is empty we fetch a fresh value from this URL (same micro-service used by Option Producer)
    @Value("${fivepaisa.totp-url:http://localhost:8002/getToto}")
    private String totpUrl;

    private final org.springframework.web.client.RestTemplate restTemplate = new org.springframework.web.client.RestTemplate();

    @Value("${fivepaisa.encrypt-key}")
    private String encryptKey;
    @Value("${fivepaisa.key}")
    private String apiKey;
    @Value("${fivepaisa.app-name}")
    private String appName;
    @Value("${fivepaisa.app-ver}")
    private String appVer;
    @Value("${fivepaisa.os-name}")
    private String osName;
    @Value("${fivepaisa.user-id}")
    private String userId;
    @Value("${fivepaisa.password}")
    private String password;

    // Configurable AppSource; default 6 for API usage, override via fivepaisa.app-source property
    @Value("${fivepaisa.app-source:6}")
    private int appSource;

    @PostConstruct
    private void init() {
        // populate AppConfig fields
        appConfig.setLoginId(loginId);
        appConfig.setUserId(userId);
        appConfig.setPassword(password);
        appConfig.setKey(apiKey);
        appConfig.setEncryptKey(encryptKey);
        appConfig.setAppName(appName);
        appConfig.setAppVer(appVer);
        appConfig.setOsName(osName);

        restClient = new RestClient(appConfig, new Properties());
        try {
            String cleanPin  = pin  != null ? pin.trim()  : "";
            if (cleanPin.isBlank()) {
                throw new IllegalStateException("fivepaisa.pin is blank – cannot establish session");
            }

            String cleanTotp;
            if (totp != null && !totp.trim().isEmpty()) {
                cleanTotp = totp.trim();
            } else {
                // Fetch fresh TOTP from micro-service
                try {
                    String res = restTemplate.getForObject(totpUrl, String.class);
                    cleanTotp = res != null ? res.replace("\"", "").trim() : "";
                    log.info("Fetched TOTP '{}' from {}", cleanTotp, totpUrl);
                } catch (Exception e) {
                    throw new IllegalStateException("Unable to fetch TOTP from " + totpUrl + ": " + e.getMessage(), e);
                }
            }

            if (cleanTotp.isBlank()) {
                throw new IllegalStateException("TOTP is blank even after fetch – cannot establish session");
            }

            String tokenResp = restClient.getTotpSession(loginId.trim(), cleanTotp, cleanPin);
            if (tokenResp == null || tokenResp.isBlank()) {
                throw new IllegalStateException("5Paisa TOTP session response was empty – check credentials / TOTP validity");
            }
            log.info("✅ 5Paisa session initialised successfully");
        } catch (Exception e) {
            throw new RuntimeException("Failed to obtain 5Paisa session", e);
        }
    }

    @Override
    public String placeMarketOrder(String scripCode, String exch, String exchType, Side side, int quantity) throws BrokerException {
        try {
            JSONObject order = buildOrderPayload(scripCode, exch, exchType, side, quantity, /*price*/0, /*atMarket*/ true);
            okhttp3.Response resp = restClient.placeOrderRequest(order);
            if (!resp.isSuccessful()) {
                throw new BrokerException("Order failed HTTP status " + resp.code());
            }
            String body = resp.body() != null ? resp.body().string() : "";
            log.info("🚀 5Paisa order placed. Response: {}", body);
            // In real use parse JSON and extract order ID; placeholder here
            return body;
        } catch (Exception ex) {
            log.error("❌ Broker order failed: {}", ex.getMessage());
            throw new BrokerException("Broker order failed", ex);
        }
    }

    @Override
    public String placeLimitOrder(String scripCode, String exch, String exchType, Side side, int quantity, double price) throws BrokerException {
        try {
            JSONObject order = buildOrderPayload(scripCode, exch, exchType, side, quantity, price, /*atMarket*/ false);
            okhttp3.Response resp = restClient.placeOrderRequest(order);
            if (!resp.isSuccessful()) {
                throw new BrokerException("Order failed HTTP status " + resp.code());
            }
            String body = resp.body() != null ? resp.body().string() : "";
            log.info("🚀 5Paisa limit order placed. Response: {}", body);
            return body;
        } catch (Exception ex) {
            throw new BrokerException("Broker limit order failed", ex);
        }
    }

    @Override
    public void squareOffPosition(String scripCode, String exch, String exchType, Side currentSide, int remainingQty) throws BrokerException {
        Side exitSide = currentSide == Side.BUY ? Side.SELL : Side.BUY;
        placeMarketOrder(scripCode, exch, exchType, exitSide, remainingQty);
    }

    @Override
    public void squareOffAll() throws BrokerException {
        // VERY SIMPLE IMPLEMENTATION – send squareOffPosition for each open trade via option "NetPosition" if needed.
        // For now, rely on internal ActiveTrade context (single trade) – call through consumer later.
        log.warn("squareOffAll() called but not yet fully implemented – no-op");
    }

    // ---------------------------------------------------------------------
    private JSONObject buildOrderPayload(String scripCode, String exch, String exchType, Side side, int qty, double price, boolean atMarket) {
        JSONObject obj = new JSONObject();
        obj.put("ClientCode", loginId);
        obj.put("Exch", exch);          // "N" or "B" etc.
        obj.put("ExchType", exchType);  // "C" cash, "D" derivative
        obj.put("ScripCode", Integer.parseInt(scripCode));
        obj.put("Qty", qty);
        obj.put("Price", price);
        obj.put("OrderType", side == Side.BUY ? "BUY" : "SELL");
        obj.put("OrderFor", "P");       // New order
        obj.put("AtMarket", atMarket);
        obj.put("RemoteOrderID", "kotsin-" + System.currentTimeMillis());
        obj.put("DisQty", 0);
        obj.put("IsStopLossOrder", false);
        obj.put("IsVTD", false);
        obj.put("IOCOrder", false);
        obj.put("IsIntraday", false);
        obj.put("PublicIP", "127.0.0.1");
        obj.put("AHPlaced", "N");
        obj.put("ValidTillDate", "/Date(0)/");
        obj.put("iOrderValidity", 0);
        obj.put("OrderRequesterCode", loginId);
        obj.put("TradedQty", 0);
        obj.put("AppSource", appSource);
        return obj;
    }
} 