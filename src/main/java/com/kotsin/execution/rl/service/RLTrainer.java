package com.kotsin.execution.rl.service;

import com.kotsin.execution.model.BacktestTrade;
import com.kotsin.execution.rl.model.*;
import com.kotsin.execution.rl.repository.RLExperienceRepository;
import com.kotsin.execution.rl.repository.RLModuleInsightRepository;
import com.kotsin.execution.rl.repository.RLPolicyRepository;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * RL Trainer - Handles batch training and online updates
 * 
 * Training modes:
 * 1. Batch: Train on stored experiences (scheduled)
 * 2. Online: Update after each new trade
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RLTrainer {
    
    private final ActorCriticNetwork network;
    private final ExperienceReplayBuffer buffer;
    private final RLStateEncoder stateEncoder;
    private final RLRewardCalculator rewardCalculator;
    private final RLPolicyRepository policyRepository;
    private final RLExperienceRepository experienceRepository;
    private final RLModuleInsightRepository moduleInsightRepository;
    
    private static final int BATCH_SIZE = 32;
    private static final int MIN_EXPERIENCES_FOR_TRAINING = 50;
    
    private RLPolicy currentPolicy;
    private boolean isInitialized = false;
    
    @PostConstruct
    public void init() {
        // Load existing policy or create new
        currentPolicy = policyRepository.findByActiveTrue()
            .orElseGet(RLPolicy::createInitial);
        
        // Load network weights if exist
        if (currentPolicy.getActorWeights() != null) {
            network.loadFromPolicy(currentPolicy);
            log.info("Loaded existing policy version {}", currentPolicy.getVersion());
        } else {
            log.info("Initialized new policy");
        }
        
        // Refresh buffer cache
        buffer.refreshCache();
        
        isInitialized = true;
        log.info("RL Trainer initialized. Experiences in buffer: {}", buffer.size());
    }
    
    /**
     * Record experience from completed backtest trade
     */
    public void recordBacktestExperience(BacktestTrade trade) {
        if (trade == null || trade.getStatus() == BacktestTrade.TradeStatus.PENDING) {
            return;
        }
        
        // Encode state
        TradeState state = stateEncoder.encode(trade);
        
        // Get action (from signal - no adjustments for now)
        TradeAction action = TradeAction.defaultAction();
        
        // Calculate reward
        double reward = rewardCalculator.calculate(trade);
        
        // Create experience
        RLExperience exp = RLExperience.builder()
            .tradeId(trade.getId())
            .scripCode(trade.getScripCode())
            .signalType(trade.getSignalType())
            .state(state.toArray())
            .action(action.toArray())
            .reward(reward)
            .rMultiple(trade.getRMultiple())
            .won(trade.getRMultiple() > 0)
            .exitReason(trade.getExitReason())
            .source(RLExperience.ExperienceSource.BACKTEST)
            .sampleWeight(1.0)
            .entryPrice(trade.getEntryPrice())
            .exitPrice(trade.getExitPrice())
            .profitLoss(trade.getProfit())
            .signalTime(trade.getSignalTime())
            .entryTime(trade.getEntryTime())
            .exitTime(trade.getExitTime())
            .createdAt(LocalDateTime.now())
            .build();
        
        buffer.add(exp);
        
        // Update policy stats
        currentPolicy.setTotalExperiences(currentPolicy.getTotalExperiences() + 1);
        currentPolicy.setBacktestExperiences(currentPolicy.getBacktestExperiences() + 1);
        
        log.debug("Recorded backtest experience: {} reward={} R={}", 
            trade.getScripCode(), reward, trade.getRMultiple());
        
        // Perform mini-batch update if buffer is ready
        if (buffer.size() >= MIN_EXPERIENCES_FOR_TRAINING && buffer.size() % 10 == 0) {
            trainMiniBatch();
        }
    }
    
    /**
     * Record experience from live trade (2x weight)
     */
    public void recordLiveTradeExperience(String tradeId, double[] state, double[] action,
                                          double reward, double rMultiple, String exitReason) {
        RLExperience exp = RLExperience.builder()
            .tradeId(tradeId)
            .state(state)
            .action(action)
            .reward(reward)
            .rMultiple(rMultiple)
            .won(rMultiple > 0)
            .exitReason(exitReason)
            .source(RLExperience.ExperienceSource.LIVE_TRADE)
            .sampleWeight(2.0)  // 2x weight for live trades
            .createdAt(LocalDateTime.now())
            .build();
        
        buffer.add(exp);
        
        currentPolicy.setTotalExperiences(currentPolicy.getTotalExperiences() + 1);
        currentPolicy.setLiveTradeExperiences(currentPolicy.getLiveTradeExperiences() + 1);
        
        log.info("Recorded LIVE trade experience: reward={} R={}", reward, rMultiple);
        
        // Always train on live experience
        trainMiniBatch();
    }
    
    /**
     * Train on mini-batch of experiences
     */
    public void trainMiniBatch() {
        if (!buffer.isReady()) {
            log.debug("Buffer not ready for training. Size: {}", buffer.size());
            return;
        }
        
        List<RLExperience> batch = buffer.sample(BATCH_SIZE);
        
        if (batch.isEmpty()) {
            return;
        }
        
        int updates = 0;
        double totalLoss = 0;
        
        for (RLExperience exp : batch) {
            double[] state = exp.getState();
            double[] action = exp.getAction();
            double reward = exp.getReward();
            
            // Critic update: TD error
            double value = network.criticForward(state);
            double target = reward;  // Terminal state
            double advantage = target - value;
            
            network.updateCritic(state, target);
            
            // Actor update: policy gradient with weighted sampling
            double weightedAdvantage = advantage * exp.getSampleWeight();
            network.updateActor(state, action, weightedAdvantage);
            
            totalLoss += Math.abs(advantage);
            updates++;
        }
        
        log.debug("Mini-batch training: {} updates, avg loss: {}",
            updates, String.format("%.4f", totalLoss / updates));
    }
    
    /**
     * Scheduled batch training (every 5 minutes)
     */
    @Scheduled(fixedRate = 300000)  // 5 minutes
    public void scheduledBatchTraining() {
        if (!isInitialized || buffer.size() < MIN_EXPERIENCES_FOR_TRAINING) {
            return;
        }

        log.debug("Starting scheduled batch training...");

        // Multiple passes
        for (int epoch = 0; epoch < 5; epoch++) {
            trainMiniBatch();
        }

        // Update module insights
        updateModuleInsights();

        // Save policy
        savePolicy();

        log.debug("Batch training complete. Policy saved.");
    }
    
    /**
     * Update module importance insights from gradient analysis
     */
    public void updateModuleInsights() {
        double[] gradients = network.getInputGradients();
        String[] moduleNames = stateEncoder.getModuleNames();
        
        for (int i = 0; i < Math.min(gradients.length, moduleNames.length); i++) {
            String moduleName = moduleNames[i];
            double importance = gradients[i];
            
            RLModuleInsight insight = moduleInsightRepository.findByModuleName(moduleName)
                .orElse(RLModuleInsight.builder()
                    .moduleName(moduleName)
                    .category(getCategory(i))
                    .build());
            
            insight.setGradientImportance(importance);
            insight.setLastUpdated(LocalDateTime.now());
            
            // Compute statistics from experiences
            computeModuleStats(insight, i);
            
            insight.computeVerdict();
            moduleInsightRepository.save(insight);
        }
        
        // Store in policy
        Map<String, Double> moduleImportance = new HashMap<>();
        for (int i = 0; i < Math.min(gradients.length, moduleNames.length); i++) {
            moduleImportance.put(moduleNames[i], gradients[i]);
        }
        currentPolicy.setModuleImportance(moduleImportance);
        
        network.resetGradientTracking();
    }
    
    /**
     * Compute module-specific statistics
     */
    private void computeModuleStats(RLModuleInsight insight, int moduleIndex) {
        List<RLExperience> experiences = experienceRepository.findTop1000ByOrderByCreatedAtDesc();
        
        if (experiences.size() < 30) {
            insight.setTradesWhenHigh(0);
            insight.setTradesWhenLow(0);
            return;
        }
        
        // Find median for threshold
        double[] values = experiences.stream()
            .mapToDouble(e -> e.getState()[moduleIndex])
            .sorted()
            .toArray();
        double median = values[values.length / 2];
        
        // Compute stats for high/low groups
        int highCount = 0, lowCount = 0;
        int highWins = 0, lowWins = 0;
        double highRSum = 0, lowRSum = 0;
        
        for (RLExperience exp : experiences) {
            double val = exp.getState()[moduleIndex];
            if (val >= median) {
                highCount++;
                if (exp.isWon()) highWins++;
                highRSum += exp.getRMultiple();
            } else {
                lowCount++;
                if (exp.isWon()) lowWins++;
                lowRSum += exp.getRMultiple();
            }
        }
        
        insight.setOptimalThreshold(median);
        insight.setTradesWhenHigh(highCount);
        insight.setTradesWhenLow(lowCount);
        insight.setWinRateWhenHigh(highCount > 0 ? (double) highWins / highCount : 0);
        insight.setWinRateWhenLow(lowCount > 0 ? (double) lowWins / lowCount : 0);
        insight.setAvgRMultipleWhenHigh(highCount > 0 ? highRSum / highCount : 0);
        insight.setAvgRMultipleWhenLow(lowCount > 0 ? lowRSum / lowCount : 0);
        
        // Simple p-value approximation
        if (highCount >= 10 && lowCount >= 10) {
            double winDiff = insight.getWinRateWhenHigh() - insight.getWinRateWhenLow();
            double pooledP = (double)(highWins + lowWins) / (highCount + lowCount);
            double se = Math.sqrt(pooledP * (1-pooledP) * (1.0/highCount + 1.0/lowCount));
            double z = se > 0 ? Math.abs(winDiff) / se : 0;
            insight.setPValue(z > 2.58 ? 0.01 : (z > 1.96 ? 0.05 : (z > 1.64 ? 0.10 : 0.5)));
        }
    }
    
    /**
     * Get category for module index
     */
    private String getCategory(int index) {
        if (index < 5) return "VCP";
        if (index < 12) return "IPU";
        if (index < 16) return "FLOW";
        return "CONTEXT";
    }
    
    /**
     * Save current policy to database
     */
    public void savePolicy() {
        network.saveToPolicy(currentPolicy);
        currentPolicy.setLastTrainedAt(LocalDateTime.now());
        currentPolicy.setLastUpdatedAt(LocalDateTime.now());
        
        // Compute recent stats
        ExperienceReplayBuffer.BufferStats stats = buffer.getStats();
        currentPolicy.setAvgReward(stats.avgReward());
        currentPolicy.setRecentWinRate(stats.winRate());
        
        currentPolicy = policyRepository.save(currentPolicy);

        log.debug("Policy saved: version={}, experiences={}, winRate={}%",
            currentPolicy.getVersion(), currentPolicy.getTotalExperiences(),
            String.format("%.2f", currentPolicy.getRecentWinRate() * 100));
    }
    
    /**
     * Get action from policy for a signal
     */
    public TradeAction getAction(TradeState state) {
        return network.getAction(state);
    }
    
    /**
     * Get current policy
     */
    public RLPolicy getCurrentPolicy() {
        return currentPolicy;
    }
    
    /**
     * Trigger manual training
     */
    public void triggerTraining(int epochs) {
        log.info("Manual training triggered for {} epochs", epochs);
        for (int i = 0; i < epochs; i++) {
            trainMiniBatch();
        }
        updateModuleInsights();
        savePolicy();
    }
}
