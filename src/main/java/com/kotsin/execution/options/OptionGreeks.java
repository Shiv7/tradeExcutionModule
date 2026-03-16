package com.kotsin.execution.options;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.time.LocalDate;

/**
 * OptionGreeks - Greek values for an option (copied from Streaming Candle for deferred Greek computation).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OptionGreeks {

    private String symbol;
    private String underlyingSymbol;
    private String scripCode;
    private double strikePrice;
    private OptionType optionType;
    private LocalDate expiry;
    private int daysToExpiry;

    private Instant timestamp;

    private double optionPrice;
    private double underlyingPrice;
    private double bidPrice;
    private double askPrice;

    private double delta;
    private double gamma;
    private double theta;
    private double vega;
    private double rho;

    private double vanna;
    private double charm;
    private double vomma;

    private double impliedVolatility;
    private double historicalVolatility;
    private double ivPercentile;
    private double ivSkew;

    private double intrinsicValue;
    private double timeValue;
    private double moneyness;
    private MoneynessType moneynessType;

    private double theoreticalPrice;
    private double mispricing;

    private double maxPain;
    private double breakeven;
    private double leverage;

    public enum OptionType {
        CALL, PUT
    }

    public enum MoneynessType {
        DEEP_ITM, ITM, ATM, OTM, DEEP_OTM
    }

    public boolean isCall() { return optionType == OptionType.CALL; }
    public boolean isPut() { return optionType == OptionType.PUT; }

    public boolean isITM() {
        return isCall() ? underlyingPrice > strikePrice : underlyingPrice < strikePrice;
    }

    public boolean isATM() {
        return Math.abs(underlyingPrice - strikePrice) / underlyingPrice < 0.01;
    }

    public boolean isOTM() { return !isITM() && !isATM(); }
    public double getAbsDelta() { return Math.abs(delta); }
    public boolean isHighGamma() { return gamma > 0.05; }
    public boolean isDecaying() { return theta < -0.5; }
}
