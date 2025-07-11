package com.kotsin.execution.broker;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Duration;

@Service
@Slf4j
public class FivePaisaBrokerService implements BrokerOrderService {

    private static final String BASE_URL = "https://Openapi.5paisa.com/VendorsAPI/Service1.svc/";

    private final OkHttpClient http = new OkHttpClient.Builder()
            .callTimeout(Duration.ofSeconds(15))
            .build();

    // ---------------------------------------------------------------------
    // Credentials injected from application.properties
    @Value("${fivepaisa.login-id}")
    private String loginId;
    @Value("${fivepaisa.user-id}")
    private String userId;
    @Value("${fivepaisa.password}")
    private String password; // password is currently unused in TOTP flow but kept for completeness
    @Value("${fivepaisa.key}")
    private String apiKey;
    @Value("${fivepaisa.encrypt-key}")
    private String encryptKey;
    @Value("${fivepaisa.pin}")
    private String pin;
    @Value("${fivepaisa.totp}")
    private String totp; // optional static totp (mostly kept empty so we fetch dynamically)
    @Value("${fivepaisa.totp-url:http://localhost:8002/getToto}")
    private String totpUrl;

    // Configurable AppSource; default 6 (public API) – overridden to 23312 via properties
    @Value("${fivepaisa.app-source:6}")
    private int appSource;

    private String accessToken; // Bearer token for subsequent calls

    // ---------------------------------------------------------------------
    @PostConstruct
    private void init() {
        authenticate();
    }

    // ---------------------------------------------------------------------
    // Public API (BrokerOrderService)
    // ---------------------------------------------------------------------
    @Override
    public String placeMarketOrder(String scripCode, String exch, String exchType, Side side, int quantity) throws BrokerException {
        try {
            JSONObject payload = buildOrderPayload(scripCode, exch, exchType, side, quantity, 0, /*isIntraday*/ false);
            return sendOrderRequest(payload);
        } catch (Exception e) {
            throw new BrokerException("Market order failed", e);
        }
    }

    @Override
    public String placeLimitOrder(String scripCode, String exch, String exchType, Side side, int quantity, double price) throws BrokerException {
        try {
            JSONObject payload = buildOrderPayload(scripCode, exch, exchType, side, quantity, price, /*isIntraday*/ false);
            return sendOrderRequest(payload);
        } catch (Exception e) {
            throw new BrokerException("Limit order failed", e);
        }
    }

    @Override
    public void squareOffPosition(String scripCode, String exch, String exchType, Side currentSide, int remainingQty) throws BrokerException {
        Side exitSide = currentSide == Side.BUY ? Side.SELL : Side.BUY;
        placeMarketOrder(scripCode, exch, exchType, exitSide, remainingQty);
    }

    @Override
    public void squareOffAll() throws BrokerException {
        // TODO: full portfolio square-off via OrderBook once implemented
        log.warn("squareOffAll() not yet implemented – skipping");
    }

    // ---------------------------------------------------------------------
    // Authentication helpers
    // ---------------------------------------------------------------------
    private void authenticate() {
        try {
            String currentTotp = (totp != null && !totp.trim().isEmpty()) ? totp.trim() : fetchTotp();
            String requestToken = totpLogin(currentTotp);
            this.accessToken = getAccessToken(requestToken);
            if (accessToken == null || accessToken.isBlank()) {
                throw new IllegalStateException("AccessToken was blank – auth failed");
            }
            log.info("✅ 5Paisa authentication successful");
        } catch (Exception e) {
            throw new IllegalStateException("Unable to authenticate with 5Paisa", e);
        }
    }

    private void ensureAuthenticated() {
        if (accessToken == null || accessToken.isBlank()) {
            authenticate();
        }
    }

    private String fetchTotp() throws IOException {
        Request req = new Request.Builder().url(totpUrl).build();
        try (Response res = http.newCall(req).execute()) {
            if (!res.isSuccessful()) throw new IOException("TOTP service HTTP " + res.code());
            return res.body() != null ? res.body().string().replace("\"", "").trim() : "";
        }
    }

    private String totpLogin(String currentTotp) throws Exception {
        JSONObject head = new JSONObject();
        head.put("Key", apiKey);
        JSONObject body = new JSONObject();
        body.put("Email_ID", loginId);
        body.put("TOTP", currentTotp);
        body.put("PIN", pin);
        body.put("PublicIP", "127.0.0.1");
        body.put("LocalIP", "127.0.0.1");
        JSONObject reqObj = new JSONObject();
        reqObj.put("head", head);
        reqObj.put("body", body);

        Request req = new Request.Builder()
                .url(BASE_URL + "TOTPLogin")
                .post(RequestBody.create(reqObj.toJSONString(), MediaType.parse("application/json")))
                .build();
        try (Response res = http.newCall(req).execute()) {
            String resp = res.body() != null ? res.body().string() : "";
            JSONParser parser = new JSONParser();
            JSONObject json = (JSONObject) parser.parse(resp);
            JSONObject h = (JSONObject) json.get("head");
            if (h == null || !"0".equals(String.valueOf(h.get("Status")))) {
                throw new IOException("TOTPLogin failed: " + resp);
            }
            return (String) ((JSONObject) json.get("body")).get("RequestToken");
        }
    }

    private String getAccessToken(String requestToken) throws Exception {
        JSONObject head = new JSONObject();
        head.put("Key", apiKey);
        JSONObject body = new JSONObject();
        body.put("RequestToken", requestToken);
        body.put("EncryKey", encryptKey);
        body.put("UserId", userId);
        body.put("PublicIP", "127.0.0.1");
        body.put("LocalIP", "127.0.0.1");
        JSONObject reqObj = new JSONObject();
        reqObj.put("head", head);
        reqObj.put("body", body);

        Request req = new Request.Builder()
                .url(BASE_URL + "GetAccessToken")
                .post(RequestBody.create(reqObj.toJSONString(), MediaType.parse("application/json")))
                .build();
        try (Response res = http.newCall(req).execute()) {
            String resp = res.body() != null ? res.body().string() : "";
            JSONParser parser = new JSONParser();
            JSONObject json = (JSONObject) parser.parse(resp);
            JSONObject b = (JSONObject) json.get("body");
            if (b == null) throw new IOException("No body in AccessToken response");
            return (String) b.get("AccessToken");
        }
    }

    // ---------------------------------------------------------------------
    // Order helpers
    // ---------------------------------------------------------------------
    private JSONObject buildOrderPayload(String scripCode, String exch, String exchType, Side side, int qty, double price, boolean isIntraday) {
        JSONObject head = new JSONObject();
        head.put("key", apiKey);

        JSONObject body = new JSONObject();
        body.put("Exchange", exch);
        body.put("ExchangeType", exchType);
        body.put("ScripCode", scripCode);
        body.put("Price", price);
        body.put("OrderType", side == Side.BUY ? "Buy" : "Sell");
        body.put("Qty", qty);
        body.put("DisQty", 0);
        body.put("IsIntraday", isIntraday);
        body.put("AHPlaced", "N");
        body.put("RemoteOrderID", "kotsin-" + System.currentTimeMillis());
        body.put("AppSource", appSource);
        body.put("iOrderValidity", 0);

        JSONObject req = new JSONObject();
        req.put("head", head);
        req.put("body", body);
        return req;
    }

    private String sendOrderRequest(JSONObject payload) throws Exception {
        ensureAuthenticated();
        Request req = new Request.Builder()
                .url(BASE_URL + "V1/PlaceOrderRequest")
                .addHeader("Authorization", "Bearer " + accessToken)
                .post(RequestBody.create(payload.toJSONString(), MediaType.parse("application/json")))
                .build();
        try (Response res = http.newCall(req).execute()) {
            String respStr = res.body() != null ? res.body().string() : "";
            if (!res.isSuccessful()) throw new IOException("HTTP " + res.code() + ": " + respStr);
            JSONParser parser = new JSONParser();
            JSONObject respJson = (JSONObject) parser.parse(respStr);
            JSONObject body = (JSONObject) respJson.get("body");
            if (body == null) throw new IOException("Malformed response: " + respStr);
            Number status = (Number) body.get("Status");
            if (status != null && status.intValue() == 0) {
                return String.valueOf(body.get("BrokerOrderID"));
            } else {
                String msg = String.valueOf(body.get("Message"));
                throw new IOException("Order rejected: " + msg);
            }
        }
    }

    // ---------------------------------------------------------------------
    // Optional helper – fetch today's order book (V4) for future extensions
    // ---------------------------------------------------------------------
    public JSONObject fetchOrderBook() throws BrokerException {
        try {
            ensureAuthenticated();
            JSONObject head = new JSONObject();
            head.put("key", apiKey);
            JSONObject body = new JSONObject();
            body.put("ClientCode", loginId);
            JSONObject reqObj = new JSONObject();
            reqObj.put("head", head);
            reqObj.put("body", body);

            Request req = new Request.Builder()
                    .url(BASE_URL + "V4/OrderBook")
                    .addHeader("Authorization", "Bearer " + accessToken)
                    .post(RequestBody.create(reqObj.toJSONString(), MediaType.parse("application/json")))
                    .build();
            try (Response res = http.newCall(req).execute()) {
                String resp = res.body() != null ? res.body().string() : "";
                JSONParser parser = new JSONParser();
                return (JSONObject) parser.parse(resp);
            }
        } catch (Exception e) {
            throw new BrokerException("Failed to fetch order book", e);
        }
    }
} 