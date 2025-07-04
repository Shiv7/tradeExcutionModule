package com.kotsin.execution.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

/**
 * Market data model for trade execution module.
 * Matches the MarketData structure from optionProducerJava module.
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class MarketData {
    
    @JsonProperty("Exch")
    private String exchange;

    @JsonProperty("ExchType")
    private String exchangeType;

    /**
     * 🔗 CRITICAL: Token is the unique identifier that links to scripCode
     * Token (market data) = scripCode (strategy signals) = unique company ID
     */
    @JsonProperty("Token")
    private int token;

    @JsonProperty("LastRate")
    private double lastRate;

    @JsonProperty("LastQty")
    private int lastQuantity;

    @JsonProperty("TotalQty")
    private int totalQuantity;

    @JsonProperty("High")
    private double high;

    @JsonProperty("Low")
    private double low;

    @JsonProperty("OpenRate")
    private double openRate;

    @JsonProperty("PClose")
    private double previousClose;

    @JsonProperty("AvgRate")
    private double averageRate;

    @JsonProperty("Time")
    private long time;

    @JsonProperty("BidQty")
    private int bidQuantity;

    @JsonProperty("BidRate")
    private double bidRate;

    @JsonProperty("OffQty")
    private int offerQuantity;

    @JsonProperty("OffRate")
    private double offerRate;

    @JsonProperty("TBidQ")
    private int totalBidQuantity;

    @JsonProperty("TOffQ")
    private int totalOfferQuantity;

    @JsonProperty("TickDt")
    private String tickDt;

    @JsonProperty("ChgPcnt")
    private double changePercent;

    private String companyName;
    
    /**
     * Get the unique identifier for linking with strategy signals
     * Token (from market data) should match scripCode (from strategy signals)
     */
    public String getUniqueIdentifier() {
        return String.valueOf(token);
    }
    
    /**
     * Check if this market data can be linked to a strategy signal
     */
    public boolean canLinkToSignal(String scripCode) {
        return scripCode != null && scripCode.equals(getUniqueIdentifier());
    }
    
    @Override
    public String toString() {
        return String.format("MarketData{token=%d, lastRate=%.2f, exchange='%s', companyName='%s'}", 
                            token, lastRate, exchange, companyName);
    }
} 