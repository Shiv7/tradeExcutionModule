package com.kotsin.execution.broker;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.json.simple.JSONArray;

@Service
@Slf4j
public class FivePaisaBrokerService implements BrokerOrderService {

    private static final String BASE_URL = "https://Openapi.5paisa.com/VendorsAPI/Service1.svc/";

    private final OkHttpClient http = new OkHttpClient.Builder()
            .callTimeout(Duration.ofSeconds(15))
            .build();

    private final ReentrantLock loginLock = new ReentrantLock();
    private volatile long tokenExpiryEpochSeconds = 0; // unix epoch seconds

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

    // Metrics --------------------------------------------
    private final Counter requestTotal;
    private final Counter requestFailed;
    private final Timer   requestLatency;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final ConcurrentMap<String, AtomicBoolean> pendingOrders = new ConcurrentHashMap<>(); // RemoteOrderID -> stillPending
    private final ConcurrentMap<String, ScheduledFuture<?>> pollFutures = new ConcurrentHashMap<>();
    private WebSocket orderWs;

    // ---------------------------------------------------------------------
    // Spring injects MeterRegistry via constructor
    public FivePaisaBrokerService(MeterRegistry registry) {
        this.meterRegistry   = registry;
        this.requestTotal    = registry.counter("fivepaisa.requests.total");
        this.requestFailed   = registry.counter("fivepaisa.requests.failed");
        this.requestLatency  = registry.timer("fivepaisa.requests.latency");
    }

    private final MeterRegistry meterRegistry;

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
            return sendOrderRequest(exch, payload);
        } catch (Exception e) {
            throw new BrokerException("Market order failed", e);
        }
    }

    @Override
    public String placeLimitOrder(String scripCode, String exch, String exchType, Side side, int quantity, double price) throws BrokerException {
        try {
            boolean intraday = "D".equalsIgnoreCase(exchType) && "N".equalsIgnoreCase(exch);
            JSONObject payload = buildOrderPayload(scripCode, exch, exchType, side, quantity, price, intraday);
            return sendOrderRequest(exch, payload);
        } catch (Exception e) {
            throw new BrokerException("Limit order failed", e);
        }
    }

    /**
     * Place Stop-Loss LIMIT order (WithSL=Y, SLTriggerRate=price).
     * Used for commodity / options where market orders are not allowed.
     */
    @Override
    public String placeStopLossLimitOrder(String scripCode, String exch, String exchType, Side side,
                                          int quantity, double price) throws BrokerException {
        try {
            boolean intraday = "D".equalsIgnoreCase(exchType) && "N".equalsIgnoreCase(exch);
            JSONObject payload = buildOrderPayload(scripCode, exch, exchType, side, quantity, price, intraday);
            // Inject SL fields
            JSONObject body = (JSONObject) payload.get("body");
            body.put("WithSL", "Y");
            body.put("SLTriggerRate", price);
            return sendOrderRequest(exch, payload);
        } catch (Exception e) {
            throw new BrokerException("Stop-loss limit order failed", e);
        }
    }

    @Override
    public void squareOffPosition(String scripCode, String exch, String exchType, Side currentSide, int remainingQty) throws BrokerException {
        Side exitSide = currentSide == Side.BUY ? Side.SELL : Side.BUY;
        placeMarketOrder(scripCode, exch, exchType, exitSide, remainingQty);
    }

    // ---------------------------------------------------------------------
    // MODIFY / CANCEL helpers (sync)
    // ---------------------------------------------------------------------
    public void modifyOrder(String exchOrderId, Double newPrice, Integer newQty) throws BrokerException {
        try {
            ensureAuthenticated();
            JSONObject head = new JSONObject();
            head.put("key", apiKey);
            JSONObject body = new JSONObject();
            body.put("ExchOrderID", exchOrderId);
            if (newPrice != null) body.put("Price", newPrice);
            if (newQty != null) body.put("Qty", newQty);
            JSONObject req = new JSONObject();
            req.put("head", head);
            req.put("body", body);

            Request httpReq = new Request.Builder()
                    .url(BASE_URL + "V1/ModifyOrderRequest")
                    .addHeader("Authorization", "Bearer " + accessToken)
                    .post(RequestBody.create(req.toJSONString(), MediaType.parse("application/json")))
                    .build();
            try (Response res = http.newCall(httpReq).execute()) {
                if (!res.isSuccessful()) throw new IOException("HTTP " + res.code());
            }
        } catch (Exception e) {
            throw new BrokerException("Modify order failed", e);
        }
    }

    public void cancelOrder(String exchOrderId) throws BrokerException {
        try {
            ensureAuthenticated();
            JSONObject head = new JSONObject();
            head.put("key", apiKey);
            JSONObject body = new JSONObject();
            body.put("ExchOrderID", exchOrderId);
            JSONObject req = new JSONObject();
            req.put("head", head);
            req.put("body", body);

            Request httpReq = new Request.Builder()
                    .url(BASE_URL + "V1/CancelOrderRequest")
                    .addHeader("Authorization", "Bearer " + accessToken)
                    .post(RequestBody.create(req.toJSONString(), MediaType.parse("application/json")))
                    .build();
            try (Response res = http.newCall(httpReq).execute()) {
                if (!res.isSuccessful()) throw new IOException("HTTP " + res.code());
            }
        } catch (Exception e) {
            throw new BrokerException("Cancel order failed", e);
        }
    }

    // ---------------------------------------------------------------------
    // Square-off all positions using OrderBook V4
    // ---------------------------------------------------------------------
    @Override
    public void squareOffAll() throws BrokerException {
        try {
            JSONObject book = fetchOrderBook();
            JSONArray details = (JSONArray) book.get("OrderBookDetail");
            if (details == null) return;
            for (Object o : details) {
                JSONObject ord = (JSONObject) o;
                String status = String.valueOf(ord.get("OrderStatus"));
                long pending = ((Number) ord.getOrDefault("PendingQty", 0)).longValue();
                if (pending > 0 && "Pending".equalsIgnoreCase(status)) {
                    String exchOrderId = String.valueOf(ord.get("ExchOrderID"));
                    cancelOrder(exchOrderId);
                }
            }
        } catch (Exception e) {
            throw new BrokerException("squareOffAll failed", e);
        }
    }

    // ---------------------------------------------------------------------
    // Authentication helpers
    // ---------------------------------------------------------------------
    private void authenticate() {
        loginLock.lock();
        try {
            long now = System.currentTimeMillis() / 1000;
            if (now < tokenExpiryEpochSeconds - 60 && accessToken != null && !accessToken.isBlank()) {
                return; // another thread already refreshed
            }

            String currentTotp = (totp != null && !totp.trim().isEmpty()) ? totp.trim() : fetchTotp();
            String requestToken = totpLogin(currentTotp);
            this.accessToken = getAccessToken(requestToken);
            if (accessToken == null || accessToken.isBlank()) {
                throw new IllegalStateException("AccessToken was blank – auth failed");
            }

            this.tokenExpiryEpochSeconds = decodeJwtExpiry(accessToken);
            log.info("✅ 5Paisa authentication successful, token exp {}", tokenExpiryEpochSeconds);
            startOrderWebSocket();
        } catch (Exception e) {
            throw new IllegalStateException("Unable to authenticate with 5Paisa", e);
        } finally {
            loginLock.unlock();
        }
    }

    private void ensureAuthenticated() {
        long now = System.currentTimeMillis() / 1000;
        if (accessToken == null || accessToken.isBlank() || now >= tokenExpiryEpochSeconds - 60) {
            authenticate();
        }
    }

    private String fetchTotp() throws IOException {
        IOException last = null;
        for (int i = 0; i < 3; i++) {
            Request req = new Request.Builder().url(totpUrl).build();
            try (Response res = http.newCall(req).execute()) {
                if (!res.isSuccessful()) throw new IOException("HTTP " + res.code());
                return res.body() != null ? res.body().string().replace("\"", "").trim() : "";
            } catch (IOException ex) {
                last = ex;
                try { TimeUnit.MILLISECONDS.sleep(500 * (i + 1)); } catch (InterruptedException ignored) {}
            }
        }
        throw last != null ? last : new IOException("Failed to fetch TOTP");
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

    private long decodeJwtExpiry(String jwt) {
        try {
            String[] parts = jwt.split("\\.");
            if (parts.length < 2) return 0;
            String payloadJson = new String(Base64.getUrlDecoder().decode(parts[1]), StandardCharsets.UTF_8);
            JSONParser parser = new JSONParser();
            JSONObject obj = (JSONObject) parser.parse(payloadJson);
            Number exp = (Number) obj.get("exp");
            return exp != null ? exp.longValue() : 0;
        } catch (Exception e) {
            log.warn("Unable to decode JWT exp: {}", e.toString());
            return 0;
        }
    }

    // ---------------------------------------------------------------------
    // Order helpers
    // ---------------------------------------------------------------------
    private JSONObject buildOrderPayload(String scripCode, String exch, String exchType, Side side, int qty, double price, boolean isIntraday) {
        JSONObject head = new JSONObject();
        head.put("key", apiKey);

        JSONObject body = new JSONObject();
        body.put("ClientCode", loginId);
        body.put("Exchange", exch);
        body.put("ExchangeType", exchType);
        body.put("ScripCode", scripCode);
        if (price > 0) {
            body.put("Price", price);
        }
        body.put("OrderType", side == Side.BUY ? "Buy" : "Sell");
        body.put("Qty", qty);
        body.put("DisQty", 0);
        body.put("IsIntraday", isIntraday);
        body.put("AHPlaced", "N");
        body.put("RemoteOrderID", UUID.randomUUID().toString());
        body.put("AppSource", appSource);
        body.put("iOrderValidity", 0);

        JSONObject req = new JSONObject();
        req.put("head", head);
        req.put("body", body);

        // 🔍 DEBUG: Log full payload details before sending
        if (log.isDebugEnabled()) {
            log.debug("📝 BuildOrderPayload – Exchange: {}, Type: {}, Side: {}, Qty: {}, Price: {}, Intraday: {}, Scrip: {}",
                    exch, exchType, side, qty, price, isIntraday, scripCode);
            log.debug("📝 Payload JSON: {}", req.toJSONString());
        }
        return req;
    }

    private String sendOrderRequest(String exch, JSONObject payload) throws Exception {
        ensureAuthenticated();
        // 🔍 DEBUG: Log outgoing request
        if (log.isDebugEnabled()) {
            log.debug("📤 Sending order request for exch {}: {}", exch, payload.toJSONString());
        }

        Request req = new Request.Builder()
                .url(BASE_URL + "V1/PlaceOrderRequest")
                .addHeader("Authorization", "Bearer " + accessToken)
                .post(RequestBody.create(payload.toJSONString(), MediaType.parse("application/json")))
                .build();
        long start = System.nanoTime();
        try (Response res = http.newCall(req).execute()) {
            requestTotal.increment();
            String respStr = res.body() != null ? res.body().string() : "";
            if (!res.isSuccessful()) {
                requestFailed.increment();
                throw new IOException("HTTP " + res.code() + ": " + respStr);
            }

            JSONParser parser = new JSONParser();
            JSONObject respJson = (JSONObject) parser.parse(respStr);
            JSONObject head = (JSONObject) respJson.get("head");
            JSONObject body = (JSONObject) respJson.get("body");

            if (log.isDebugEnabled()) {
                log.debug("📥 Broker raw response (HTTP {}): {}", res.code(), respStr);
            }
            decodeBrokerError(head, body);

            String remoteId = String.valueOf(body.get("RemoteOrderID"));
            trackPending(exch, remoteId);
            return remoteId;
        } catch (Exception e) {
            requestFailed.increment();
            if (log.isDebugEnabled()) {
                log.debug("❌ Broker request failed: {}", e.getMessage(), e);
            }
            throw e;
        } finally {
            requestLatency.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    private void decodeBrokerError(JSONObject head, JSONObject body) throws IOException {
        if (head == null) throw new IOException("Missing head in response");
        String status = String.valueOf(head.get("status"));
        if (!"0".equals(status)) {
            throw new IOException("Broker head status=" + status);
        }
        if (body == null) return;
        Number stat = (Number) body.get("Status");
        if (stat != null && stat.intValue() != 0) {
            String msg = String.valueOf(body.get("Message"));
            throw new IOException("BrokerRMS " + msg);
        }
    }

    // ---------------------------------------------------------------------
    // Optional helper – fetch today's order book (V4) for future extensions
    // ---------------------------------------------------------------------
    public JSONObject fetchOrderBook() throws BrokerException {
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

        long startNano = System.nanoTime();
        try (Response res = http.newCall(req).execute()) {
            requestTotal.increment();
            String resp = res.body() != null ? res.body().string() : "";
            if (!res.isSuccessful()) {
                requestFailed.increment();
                throw new IOException("HTTP " + res.code() + ": " + resp);
            }

            JSONParser parser = new JSONParser();
            JSONObject respJson = (JSONObject) parser.parse(resp);
            JSONObject headResp = (JSONObject) respJson.get("head");
            JSONObject bodyResp = (JSONObject) respJson.get("body");
            decodeBrokerError(headResp, bodyResp);

            return (JSONObject) bodyResp.get("OrderBookDetail");
        } catch (Exception e) {
            requestFailed.increment();
            throw new BrokerException("Failed to fetch order book", e);
        } finally {
            requestLatency.record(System.nanoTime() - startNano, TimeUnit.NANOSECONDS);
        }
    }

    private void startOrderWebSocket() {
        try {
            String server = "C"; // default
            // decode again to find RedirectServer
            String[] parts = accessToken.split("\\.");
            if (parts.length >= 2) {
                String payloadJson = new String(Base64.getUrlDecoder().decode(parts[1]), StandardCharsets.UTF_8);
                JSONObject payload = (JSONObject) new JSONParser().parse(payloadJson);
                Object rs = payload.get("RedirectServer");
                if (rs != null) server = rs.toString();
            }
            String host = switch (server) {
                case "A" -> "aopenfeed.5paisa.com";
                case "B" -> "bopenfeed.5paisa.com";
                default -> "openfeed.5paisa.com";
            };
            String url = "wss://" + host + "/feeds/api/chat?Value1=" + accessToken + "|" + loginId;
            Request req = new Request.Builder().url(url).build();
            orderWs = http.newWebSocket(req, new WebSocketListener() {
                @Override public void onMessage(WebSocket webSocket, String text) {
                    handleWsMessage(text);
                }
                @Override public void onClosed(WebSocket ws, int code, String reason) { log.warn("Order WS closed: {}", reason); scheduleWsReconnect(); }
                @Override public void onFailure(WebSocket ws, Throwable t, Response r) { log.error("Order WS failure {}", t.toString()); scheduleWsReconnect(); }
            });
        } catch (Exception e) {
            log.warn("Failed to start order WebSocket: {}", e.toString());
            scheduleWsReconnect();
        }
    }

    private void handleWsMessage(String text) {
        try {
            JSONParser p = new JSONParser();
            JSONObject obj = (JSONObject) p.parse(text);
            String reqType = String.valueOf(obj.get("ReqType"));
            String remoteId = String.valueOf(obj.get("RemoteOrderId"));
            long pendingQty = ((Number) obj.getOrDefault("PendingQty",0)).longValue();
            if (pendingQty ==0) {
                pendingOrders.remove(remoteId);
                ScheduledFuture<?> f = pollFutures.remove(remoteId);
                if (f!=null) f.cancel(false);
            }
        } catch (Exception ignored) {}
    }

    // ------------------ order polling helpers ---------------------------
    private void trackPending(String exch, String remoteId) {
        pendingOrders.put(remoteId, new AtomicBoolean(true));
        ScheduledFuture<?> future = scheduler.scheduleAtFixedRate(() -> {
            AtomicBoolean flag = pendingOrders.get(remoteId);
            if (flag == null || !flag.get()) return;
            try {
                JSONObject status = fetchOrderStatus(exch, remoteId);
                JSONArray lst = (JSONArray) status.get("OrdStatusResLst");
                if (lst != null && !lst.isEmpty()) {
                    JSONObject s = (JSONObject) lst.get(0);
                    String st = String.valueOf(s.get("Status"));
                    if (!"Pending".equalsIgnoreCase(st)) {
                        pendingOrders.remove(remoteId);
                        long pend = ((Number) ((JSONObject) lst.get(0)).getOrDefault("PendingQty",0)).longValue();
                        if (pend == 0) {
                            pendingOrders.remove(remoteId);
                            ScheduledFuture<?> f = pollFutures.remove(remoteId);
                            if (f!=null) f.cancel(false);
                        }
                    }
                }
            } catch (Exception e) {
                log.debug("poll status error {}", e.toString());
            }
        }, 2, 2, TimeUnit.SECONDS);
        pollFutures.put(remoteId, future);
    }

    private JSONObject fetchOrderStatus(String exch, String remoteId) throws Exception {
        ensureAuthenticated();
        JSONObject head = new JSONObject();
        head.put("key", apiKey);
        JSONObject inner = new JSONObject();
        inner.put("Exch", exch);
        inner.put("RemoteOrderID", remoteId);
        org.json.simple.JSONArray arr = new org.json.simple.JSONArray();
        arr.add(inner);
        JSONObject body = new JSONObject();
        body.put("ClientCode", loginId);
        body.put("OrdStatusReqList", arr);
        JSONObject reqObj = new JSONObject();
        reqObj.put("head", head);
        reqObj.put("body", body);

        Request req = new Request.Builder()
                .url(BASE_URL + "V2/OrderStatus")
                .addHeader("Authorization", "Bearer " + accessToken)
                .post(RequestBody.create(reqObj.toJSONString(), MediaType.parse("application/json")))
                .build();
        try (Response res = http.newCall(req).execute()) {
            String respStr = res.body() != null ? res.body().string() : "";
            JSONParser parser = new JSONParser();
            JSONObject full = (JSONObject) parser.parse(respStr);
            return (JSONObject) full.get("body");
        }
    }

    private void scheduleWsReconnect() {
        scheduler.schedule(this::startOrderWebSocket,5,TimeUnit.SECONDS);
    }

    @PreDestroy
    public void shutdown() {
        try { scheduler.shutdownNow(); } catch (Exception ignored) {}
        if (orderWs!=null) {
            try { orderWs.close(1000, "shutdown"); } catch (Exception ignored) {}
        }
    }
} 