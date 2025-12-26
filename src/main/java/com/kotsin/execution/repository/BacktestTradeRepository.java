package com.kotsin.execution.repository;

import com.kotsin.execution.model.BacktestTrade;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

/**
 * BacktestTradeRepository - MongoDB operations for backtest trades
 */
@Repository
public interface BacktestTradeRepository extends MongoRepository<BacktestTrade, String> {
    
    // Find by status
    List<BacktestTrade> findByStatus(BacktestTrade.TradeStatus status);
    
    // Find completed trades
    List<BacktestTrade> findByStatusOrderByExitTimeDesc(BacktestTrade.TradeStatus status);
    
    // Find by scripCode
    List<BacktestTrade> findByScripCodeOrderBySignalTimeDesc(String scripCode);
    
    // Find by signal type
    List<BacktestTrade> findBySignalTypeOrderBySignalTimeDesc(String signalType);
    
    // Find by date range
    List<BacktestTrade> findBySignalTimeBetweenOrderBySignalTimeDesc(
            LocalDateTime from, LocalDateTime to);
    
    // Find profitable trades
    @Query("{ 'profit': { $gt: 0 }, 'status': 'COMPLETED' }")
    List<BacktestTrade> findProfitableTrades();
    
    // Find losing trades
    @Query("{ 'profit': { $lt: 0 }, 'status': 'COMPLETED' }")
    List<BacktestTrade> findLosingTrades();
    
    // Find by direction
    List<BacktestTrade> findByDirectionOrderBySignalTimeDesc(String direction);
    
    // Find high confidence trades
    @Query("{ 'confidence': { $gte: ?0 }, 'status': 'COMPLETED' }")
    List<BacktestTrade> findByMinConfidence(double minConfidence);
    
    // Find xfactor trades
    List<BacktestTrade> findByXfactorFlagTrueOrderBySignalTimeDesc();
    
    // Count by status
    long countByStatus(BacktestTrade.TradeStatus status);
    
    // Recent N trades
    List<BacktestTrade> findTop100ByStatusOrderBySignalTimeDesc(BacktestTrade.TradeStatus status);
}
