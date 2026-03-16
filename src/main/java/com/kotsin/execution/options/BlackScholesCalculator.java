package com.kotsin.execution.options;

import com.kotsin.execution.options.OptionGreeks;
import com.kotsin.execution.options.OptionGreeks.MoneynessType;
import com.kotsin.execution.options.OptionGreeks.OptionType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

/**
 * BlackScholesCalculator - copied from Streaming Candle for deferred Greek computation
 * in Trade Execution's batch evaluation path.
 */
@Component
@Slf4j
public class BlackScholesCalculator {

    @Value("${options.risk.free.rate:0.07}")
    private double riskFreeRate;

    private static final double DAYS_PER_YEAR = 365.0;
    private static final int MAX_IV_ITERATIONS = 100;
    private static final double IV_PRECISION = 0.0001;

    public OptionGreeks calculateGreeks(
            double spot, double strike, LocalDate expiry,
            OptionType optionType, double optionPrice) {
        return calculateGreeks(spot, strike, expiry, optionType, optionPrice, null);
    }

    public OptionGreeks calculateGreeks(
            double spot, double strike, LocalDate expiry,
            OptionType optionType, double optionPrice, Double knownIV) {

        int daysToExpiry = (int) ChronoUnit.DAYS.between(LocalDate.now(), expiry);
        double t = Math.max(daysToExpiry / DAYS_PER_YEAR, 0.001);

        double iv = knownIV != null ? knownIV / 100.0 :
            calculateImpliedVolatility(spot, strike, t, riskFreeRate, optionPrice, optionType);

        double d1 = calculateD1(spot, strike, t, riskFreeRate, iv);
        double d2 = d1 - iv * Math.sqrt(t);

        double nd1 = normalCDF(d1);
        double nd2 = normalCDF(d2);
        double nMinusD1 = normalCDF(-d1);
        double nMinusD2 = normalCDF(-d2);
        double npd1 = normalPDF(d1);

        double delta, theta, rho, theoreticalPrice;

        if (optionType == OptionType.CALL) {
            delta = nd1;
            theoreticalPrice = spot * nd1 - strike * Math.exp(-riskFreeRate * t) * nd2;
            theta = (-spot * npd1 * iv / (2 * Math.sqrt(t))
                    - riskFreeRate * strike * Math.exp(-riskFreeRate * t) * nd2) / DAYS_PER_YEAR;
            rho = strike * t * Math.exp(-riskFreeRate * t) * nd2 / 100;
        } else {
            delta = nd1 - 1;
            theoreticalPrice = strike * Math.exp(-riskFreeRate * t) * nMinusD2 - spot * nMinusD1;
            theta = (-spot * npd1 * iv / (2 * Math.sqrt(t))
                    + riskFreeRate * strike * Math.exp(-riskFreeRate * t) * nMinusD2) / DAYS_PER_YEAR;
            rho = -strike * t * Math.exp(-riskFreeRate * t) * nMinusD2 / 100;
        }

        double gamma = npd1 / (spot * iv * Math.sqrt(t));
        double vega = spot * npd1 * Math.sqrt(t) / 100;
        double vanna = -npd1 * d2 / iv;
        double charm = -npd1 * (riskFreeRate / (iv * Math.sqrt(t)) - d2 / (2 * t)) / DAYS_PER_YEAR;
        double vomma = vega * d1 * d2 / iv;

        double intrinsicValue = optionType == OptionType.CALL ?
            Math.max(0, spot - strike) : Math.max(0, strike - spot);
        double timeValue = optionPrice - intrinsicValue;
        double moneyness = strike / spot;
        MoneynessType moneynessType = calculateMoneynessType(spot, strike, optionType);
        double breakeven = optionType == OptionType.CALL ?
            strike + optionPrice : strike - optionPrice;
        double leverage = optionPrice > 0 ? Math.abs(delta) * spot / optionPrice : 0;

        return OptionGreeks.builder()
            .strikePrice(strike).optionType(optionType).expiry(expiry)
            .daysToExpiry(daysToExpiry).timestamp(Instant.now())
            .optionPrice(optionPrice).underlyingPrice(spot)
            .delta(delta).gamma(gamma).theta(theta).vega(vega).rho(rho)
            .vanna(vanna).charm(charm).vomma(vomma)
            .impliedVolatility(iv * 100)
            .intrinsicValue(intrinsicValue).timeValue(timeValue)
            .moneyness(moneyness).moneynessType(moneynessType)
            .theoreticalPrice(theoreticalPrice)
            .mispricing(optionPrice - theoreticalPrice)
            .breakeven(breakeven).leverage(leverage)
            .build();
    }

    public double calculatePrice(double spot, double strike, double timeYears,
                                  double volatility, OptionType optionType) {
        double d1 = calculateD1(spot, strike, timeYears, riskFreeRate, volatility);
        double d2 = d1 - volatility * Math.sqrt(timeYears);
        if (optionType == OptionType.CALL) {
            return spot * normalCDF(d1) - strike * Math.exp(-riskFreeRate * timeYears) * normalCDF(d2);
        } else {
            return strike * Math.exp(-riskFreeRate * timeYears) * normalCDF(-d2) - spot * normalCDF(-d1);
        }
    }

    public double calculateImpliedVolatility(double spot, double strike, double timeYears,
                                              double rate, double marketPrice, OptionType optionType) {
        if (marketPrice <= 0 || timeYears <= 0) return 0.20;
        double iv = Math.sqrt(2 * Math.PI / timeYears) * marketPrice / spot;
        iv = Math.max(0.01, Math.min(iv, 5.0));
        for (int i = 0; i < MAX_IV_ITERATIONS; i++) {
            double price = calculatePrice(spot, strike, timeYears, iv, optionType);
            double vega = calculateVega(spot, strike, timeYears, iv);
            if (Math.abs(vega) < 1e-10) break;
            double diff = marketPrice - price;
            if (Math.abs(diff) < IV_PRECISION) break;
            iv = iv + diff / (vega * 100);
            iv = Math.max(0.01, Math.min(iv, 5.0));
        }
        return iv;
    }

    private double calculateVega(double spot, double strike, double timeYears, double volatility) {
        double d1 = calculateD1(spot, strike, timeYears, riskFreeRate, volatility);
        return spot * normalPDF(d1) * Math.sqrt(timeYears) / 100;
    }

    private double calculateD1(double spot, double strike, double timeYears, double rate, double volatility) {
        return (Math.log(spot / strike) + (rate + volatility * volatility / 2) * timeYears)
               / (volatility * Math.sqrt(timeYears));
    }

    private double normalCDF(double x) {
        if (x > 6) return 1.0;
        if (x < -6) return 0.0;
        double a = Math.abs(x);
        double t = 1.0 / (1.0 + a * 0.2316419);
        double b = 0.39894228 * Math.exp(-x * x / 2.0);
        double n = ((((1.330274429 * t - 1.821255978) * t + 1.781477937) * t - 0.356563782) * t + 0.319381530) * t;
        n = 1.0 - b * n;
        return x < 0 ? 1.0 - n : n;
    }

    private double normalPDF(double x) {
        return Math.exp(-x * x / 2) / Math.sqrt(2 * Math.PI);
    }

    private MoneynessType calculateMoneynessType(double spot, double strike, OptionType optionType) {
        double diff = (spot - strike) / spot;
        if (optionType == OptionType.PUT) diff = -diff;
        if (diff > 0.05) return MoneynessType.DEEP_ITM;
        if (diff > 0) return MoneynessType.ITM;
        if (diff > -0.01) return MoneynessType.ATM;
        if (diff > -0.05) return MoneynessType.OTM;
        return MoneynessType.DEEP_OTM;
    }
}
