package com.kotsin.execution.ml.service;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Bayesian Adaptive Sizer using Thompson Sampling
 *
 * Maintains Beta(alpha, beta) distributions per strategy per regime.
 * Uses Thompson Sampling for exploration/exploitation balance.
 * Replaces broken Actor-Critic RL for position sizing decisions.
 *
 * SHADOW MODE: Logs recommendations but does NOT affect actual trading.
 */
@Slf4j
@Service
public class BayesianSizer {

    // Beta distribution parameters per strategy-regime key
    private final Map<String, BetaParams> distributions = new ConcurrentHashMap<>();
    private final Random random = new Random();

    // Priors initialized from 206 trade outcomes
    private static final double DEFAULT_ALPHA = 1.0;  // Prior wins
    private static final double DEFAULT_BETA = 1.0;   // Prior losses

    @PostConstruct
    public void init() {
        log.info("BayesianSizer initialized in SHADOW mode");
    }

    /**
     * Get trade probability via Thompson Sampling.
     * Samples from Beta(alpha, beta) distribution for this strategy+regime.
     *
     * @return probability (0-1) that this trade will be profitable
     */
    public double sampleTradeProbability(String strategy, String regime) {
        String key = makeKey(strategy, regime);
        BetaParams params = distributions.computeIfAbsent(key, k -> new BetaParams(DEFAULT_ALPHA, DEFAULT_BETA));

        // Thompson Sampling: sample from Beta distribution
        double sample = sampleBeta(params.alpha, params.beta);

        log.debug("bayesian_sample strategy={} regime={} alpha={} beta={} sample={}",
                strategy, regime, params.alpha, params.beta, sample);

        return sample;
    }

    /**
     * Get expected win rate (mean of Beta distribution).
     * Use for display/comparison, not for exploration.
     */
    public double getExpectedWinRate(String strategy, String regime) {
        String key = makeKey(strategy, regime);
        BetaParams params = distributions.getOrDefault(key, new BetaParams(DEFAULT_ALPHA, DEFAULT_BETA));
        return params.alpha / (params.alpha + params.beta);
    }

    /**
     * Get sizing recommendation based on Kelly criterion.
     * Kelly fraction = p - (1-p)/b where p=win prob, b=avg win/avg loss ratio
     *
     * @param strategy strategy name
     * @param regime current regime
     * @param avgWinLossRatio average win amount / average loss amount
     * @return recommended position size as fraction of available capital (0-1)
     */
    public SizingRecommendation getRecommendation(String strategy, String regime, double avgWinLossRatio) {
        double sampledProb = sampleTradeProbability(strategy, regime);
        double expectedProb = getExpectedWinRate(strategy, regime);

        String key = makeKey(strategy, regime);
        BetaParams params = distributions.getOrDefault(key, new BetaParams(DEFAULT_ALPHA, DEFAULT_BETA));
        int totalSamples = (int) (params.alpha + params.beta - 2); // Subtract priors

        // Kelly fraction (half-Kelly for safety)
        double kellyFull = sampledProb - (1.0 - sampledProb) / Math.max(0.01, avgWinLossRatio);
        double kellyHalf = Math.max(0, kellyFull) * 0.5;

        // Confidence adjustment: reduce size when few samples
        double confidenceMultiplier = Math.min(1.0, totalSamples / 50.0);
        double adjustedSize = kellyHalf * confidenceMultiplier;

        // Should trade decision
        boolean shouldTrade = sampledProb > 0.45; // Slight bias toward trading (exploration)

        return new SizingRecommendation(
                shouldTrade,
                Math.min(1.0, Math.max(0.0, adjustedSize)),
                sampledProb,
                expectedProb,
                totalSamples,
                regime
        );
    }

    /**
     * Update distribution with trade outcome (Bayesian update).
     * Call this after a trade completes.
     */
    public void recordOutcome(String strategy, String regime, boolean isWin) {
        String key = makeKey(strategy, regime);
        BetaParams params = distributions.computeIfAbsent(key, k -> new BetaParams(DEFAULT_ALPHA, DEFAULT_BETA));

        if (isWin) {
            params.alpha += 1.0;
        } else {
            params.beta += 1.0;
        }

        log.info("bayesian_update strategy={} regime={} win={} alpha={} beta={} winRate={}",
                strategy, regime, isWin, params.alpha, params.beta,
                String.format("%.1f%%", params.alpha / (params.alpha + params.beta) * 100));
    }

    /**
     * Initialize from historical data (call once with aggregated stats).
     */
    public void initializeFromHistory(String strategy, String regime, int wins, int losses) {
        String key = makeKey(strategy, regime);
        distributions.put(key, new BetaParams(DEFAULT_ALPHA + wins, DEFAULT_BETA + losses));
        log.info("bayesian_init strategy={} regime={} wins={} losses={} winRate={}",
                strategy, regime, wins, losses,
                String.format("%.1f%%", (double) wins / Math.max(1, wins + losses) * 100));
    }

    /**
     * Get all distribution parameters (for dashboard display).
     */
    public Map<String, BetaParams> getAllDistributions() {
        return new ConcurrentHashMap<>(distributions);
    }

    /**
     * Sample from Beta distribution using Gamma sampling method.
     */
    private double sampleBeta(double alpha, double beta) {
        double x = sampleGamma(alpha);
        double y = sampleGamma(beta);
        return x / (x + y);
    }

    /**
     * Sample from Gamma(alpha, 1) using Marsaglia and Tsang's method.
     */
    private double sampleGamma(double alpha) {
        if (alpha < 1.0) {
            // For alpha < 1, use Gamma(1+alpha) * U^(1/alpha)
            return sampleGamma(1.0 + alpha) * Math.pow(random.nextDouble(), 1.0 / alpha);
        }

        double d = alpha - 1.0 / 3.0;
        double c = 1.0 / Math.sqrt(9.0 * d);

        while (true) {
            double x = random.nextGaussian();
            double v = Math.pow(1.0 + c * x, 3);

            if (v <= 0) continue;

            double u = random.nextDouble();
            if (u < 1.0 - 0.0331 * Math.pow(x, 4)) return d * v;
            if (Math.log(u) < 0.5 * x * x + d * (1.0 - v + Math.log(v))) return d * v;
        }
    }

    private String makeKey(String strategy, String regime) {
        return (strategy != null ? strategy : "ALL") + ":" + (regime != null ? regime : "UNKNOWN");
    }

    @Data
    public static class BetaParams {
        private double alpha;
        private double beta;

        public BetaParams(double alpha, double beta) {
            this.alpha = alpha;
            this.beta = beta;
        }

        public double mean() { return alpha / (alpha + beta); }
        public double variance() {
            double sum = alpha + beta;
            return (alpha * beta) / (sum * sum * (sum + 1));
        }
    }

    public record SizingRecommendation(
            boolean shouldTrade,
            double recommendedSize,
            double sampledProbability,
            double expectedWinRate,
            int totalSamples,
            String regime
    ) {}
}
