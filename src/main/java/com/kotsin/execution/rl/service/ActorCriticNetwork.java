package com.kotsin.execution.rl.service;

import com.kotsin.execution.rl.model.RLPolicy;
import com.kotsin.execution.rl.model.TradeAction;
import com.kotsin.execution.rl.model.TradeState;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Pure Java Actor-Critic Neural Network
 * No external ML libraries - simple but functional
 * 
 * Architecture:
 * - Shared hidden layer: 20 → 64 (ReLU)
 * - Actor head: 64 → 32 → 4 (Tanh for bounded output)
 * - Critic head: 64 → 32 → 1 (Linear)
 */
@Slf4j
@Component
public class ActorCriticNetwork {
    
    private static final int STATE_DIM = 20;
    private static final int HIDDEN_DIM = 64;
    private static final int HEAD_DIM = 32;
    private static final int ACTION_DIM = 4;
    
    // Shared layer
    private double[][] w1;  // STATE_DIM x HIDDEN_DIM
    private double[] b1;    // HIDDEN_DIM
    
    // Actor head
    private double[][] wA1;  // HIDDEN_DIM x HEAD_DIM
    private double[] bA1;    // HEAD_DIM
    private double[][] wA2;  // HEAD_DIM x ACTION_DIM
    private double[] bA2;    // ACTION_DIM
    
    // Critic head
    private double[][] wC1;  // HIDDEN_DIM x HEAD_DIM
    private double[] bC1;    // HEAD_DIM
    private double[] wC2;    // HEAD_DIM
    private double bC2;      // scalar
    
    // Learning parameters
    private double learningRate = 0.001;
    private double gamma = 0.99;
    
    // Gradient accumulators for input importance
    private double[] inputGradientSum;
    private int gradientCount;
    
    private final Random random = new Random(42);
    
    public ActorCriticNetwork() {
        initialize();
    }
    
    /**
     * Initialize weights with Xavier initialization
     */
    private void initialize() {
        // Shared layer
        w1 = xavierInit(STATE_DIM, HIDDEN_DIM);
        b1 = new double[HIDDEN_DIM];
        
        // Actor head
        wA1 = xavierInit(HIDDEN_DIM, HEAD_DIM);
        bA1 = new double[HEAD_DIM];
        wA2 = xavierInit(HEAD_DIM, ACTION_DIM);
        bA2 = new double[ACTION_DIM];
        
        // Critic head
        wC1 = xavierInit(HIDDEN_DIM, HEAD_DIM);
        bC1 = new double[HEAD_DIM];
        wC2 = xavierInit(HEAD_DIM, 1)[0];
        bC2 = 0;
        
        // Gradient tracking
        inputGradientSum = new double[STATE_DIM];
        gradientCount = 0;
        
        log.info("Initialized Actor-Critic network: {}→{}→{}/{}", 
            STATE_DIM, HIDDEN_DIM, HEAD_DIM, ACTION_DIM);
    }
    
    /**
     * Forward pass through Actor network
     * Returns action values in range [-1, 1] for adjustments, [0, 1] for shouldTrade
     */
    public double[] actorForward(double[] state) {
        // Shared layer: state → hidden
        double[] hidden = relu(add(matmul(state, w1), b1));
        
        // Actor head: hidden → action
        double[] head = relu(add(matmul(hidden, wA1), bA1));
        double[] action = add(matmul(head, wA2), bA2);
        
        // Apply tanh for bounded output
        action = tanh(action);
        
        // Adjust shouldTrade to [0, 1] range
        action[0] = (action[0] + 1) / 2;
        
        return action;
    }
    
    /**
     * Forward pass through Critic network
     * Returns state value estimate
     */
    public double criticForward(double[] state) {
        // Shared layer: state → hidden
        double[] hidden = relu(add(matmul(state, w1), b1));
        
        // Critic head: hidden → value
        double[] head = relu(add(matmul(hidden, wC1), bC1));
        double value = dot(head, wC2) + bC2;
        
        return value;
    }
    
    /**
     * Actor convenience method
     */
    public TradeAction getAction(TradeState state) {
        double[] action = actorForward(state.toArray());
        return TradeAction.fromArray(action);
    }
    
    /**
     * Critic convenience method
     */
    public double getValue(TradeState state) {
        return criticForward(state.toArray());
    }
    
    /**
     * Update Actor using policy gradient
     * 
     * @param state Input state
     * @param action Action taken
     * @param advantage TD error (reward - value)
     */
    public void updateActor(double[] state, double[] action, double advantage) {
        // Forward pass (with caching for backprop)
        double[] hidden = relu(add(matmul(state, w1), b1));
        double[] head = relu(add(matmul(hidden, wA1), bA1));
        double[] predicted = tanh(add(matmul(head, wA2), bA2));
        
        // Simple policy gradient: push action probability in direction of advantage
        double[] actionGrad = new double[ACTION_DIM];
        for (int i = 0; i < ACTION_DIM; i++) {
            // Gradient: advantage * gradient of log probability
            // Simplified: advantage * (action - predicted) for Gaussian policy
            actionGrad[i] = advantage * (action[i] - predicted[i]);
        }
        
        // Backprop through actor head
        // wA2 gradient
        for (int i = 0; i < HEAD_DIM; i++) {
            for (int j = 0; j < ACTION_DIM; j++) {
                wA2[i][j] += learningRate * head[i] * actionGrad[j];
            }
        }
        
        // bA2 gradient
        for (int j = 0; j < ACTION_DIM; j++) {
            bA2[j] += learningRate * actionGrad[j];
        }
        
        // Track input gradients for feature importance
        trackInputGradients(state, advantage);
    }
    
    /**
     * Update Critic using TD error
     */
    public void updateCritic(double[] state, double target) {
        double predicted = criticForward(state);
        double error = target - predicted;
        
        // Forward pass with caching
        double[] hidden = relu(add(matmul(state, w1), b1));
        double[] head = relu(add(matmul(hidden, wC1), bC1));
        
        // wC2 gradient
        for (int i = 0; i < HEAD_DIM; i++) {
            wC2[i] += learningRate * head[i] * error;
        }
        
        // bC2 gradient
        bC2 += learningRate * error;
    }
    
    /**
     * Track which input features contribute most to decisions
     */
    private void trackInputGradients(double[] state, double advantage) {
        // Simple approximation: weight input by advantage magnitude
        for (int i = 0; i < STATE_DIM; i++) {
            inputGradientSum[i] += Math.abs(state[i] * advantage);
        }
        gradientCount++;
    }
    
    /**
     * Get input feature importance (normalized)
     */
    public double[] getInputGradients() {
        if (gradientCount == 0) return new double[STATE_DIM];
        
        double[] importance = new double[STATE_DIM];
        double sum = 0;
        
        for (int i = 0; i < STATE_DIM; i++) {
            importance[i] = inputGradientSum[i] / gradientCount;
            sum += importance[i];
        }
        
        // Normalize to sum to 1
        if (sum > 0) {
            for (int i = 0; i < STATE_DIM; i++) {
                importance[i] /= sum;
            }
        }
        
        return importance;
    }
    
    /**
     * Reset gradient accumulators
     */
    public void resetGradientTracking() {
        Arrays.fill(inputGradientSum, 0);
        gradientCount = 0;
    }
    
    /**
     * Save weights to policy
     */
    public void saveToPolicy(RLPolicy policy) {
        Map<String, double[]> actorWeights = new HashMap<>();
        actorWeights.put("w1", flatten(w1));
        actorWeights.put("b1", b1);
        actorWeights.put("wA1", flatten(wA1));
        actorWeights.put("bA1", bA1);
        actorWeights.put("wA2", flatten(wA2));
        actorWeights.put("bA2", bA2);
        
        Map<String, double[]> criticWeights = new HashMap<>();
        criticWeights.put("wC1", flatten(wC1));
        criticWeights.put("bC1", bC1);
        criticWeights.put("wC2", wC2);
        criticWeights.put("bC2", new double[]{bC2});
        
        policy.setActorWeights(actorWeights);
        policy.setCriticWeights(criticWeights);
    }
    
    /**
     * Load weights from policy
     */
    public void loadFromPolicy(RLPolicy policy) {
        if (policy.getActorWeights() == null) return;
        
        Map<String, double[]> actorWeights = policy.getActorWeights();
        w1 = unflatten(actorWeights.get("w1"), STATE_DIM, HIDDEN_DIM);
        b1 = actorWeights.get("b1");
        wA1 = unflatten(actorWeights.get("wA1"), HIDDEN_DIM, HEAD_DIM);
        bA1 = actorWeights.get("bA1");
        wA2 = unflatten(actorWeights.get("wA2"), HEAD_DIM, ACTION_DIM);
        bA2 = actorWeights.get("bA2");
        
        Map<String, double[]> criticWeights = policy.getCriticWeights();
        if (criticWeights != null) {
            wC1 = unflatten(criticWeights.get("wC1"), HIDDEN_DIM, HEAD_DIM);
            bC1 = criticWeights.get("bC1");
            wC2 = criticWeights.get("wC2");
            bC2 = criticWeights.get("bC2")[0];
        }
        
        log.info("Loaded weights from policy version {}", policy.getVersion());
    }
    
    // === Math utilities ===
    
    private double[][] xavierInit(int in, int out) {
        double[][] w = new double[in][out];
        double scale = Math.sqrt(2.0 / (in + out));
        for (int i = 0; i < in; i++) {
            for (int j = 0; j < out; j++) {
                w[i][j] = random.nextGaussian() * scale;
            }
        }
        return w;
    }
    
    private double[] matmul(double[] v, double[][] m) {
        double[] result = new double[m[0].length];
        for (int j = 0; j < m[0].length; j++) {
            for (int i = 0; i < v.length; i++) {
                result[j] += v[i] * m[i][j];
            }
        }
        return result;
    }
    
    private double[] add(double[] a, double[] b) {
        double[] result = new double[a.length];
        for (int i = 0; i < a.length; i++) {
            result[i] = a[i] + b[i];
        }
        return result;
    }
    
    private double[] relu(double[] x) {
        double[] result = new double[x.length];
        for (int i = 0; i < x.length; i++) {
            result[i] = Math.max(0, x[i]);
        }
        return result;
    }
    
    private double[] tanh(double[] x) {
        double[] result = new double[x.length];
        for (int i = 0; i < x.length; i++) {
            result[i] = Math.tanh(x[i]);
        }
        return result;
    }
    
    private double dot(double[] a, double[] b) {
        double sum = 0;
        for (int i = 0; i < a.length; i++) {
            sum += a[i] * b[i];
        }
        return sum;
    }
    
    private double[] flatten(double[][] m) {
        double[] result = new double[m.length * m[0].length];
        int idx = 0;
        for (double[] row : m) {
            for (double v : row) {
                result[idx++] = v;
            }
        }
        return result;
    }
    
    private double[][] unflatten(double[] v, int rows, int cols) {
        double[][] m = new double[rows][cols];
        int idx = 0;
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                m[i][j] = v[idx++];
            }
        }
        return m;
    }
    
    public void setLearningRate(double lr) {
        this.learningRate = lr;
    }
    
    public double getLearningRate() {
        return learningRate;
    }
}
