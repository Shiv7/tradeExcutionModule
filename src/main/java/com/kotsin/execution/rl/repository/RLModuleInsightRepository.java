package com.kotsin.execution.rl.repository;

import com.kotsin.execution.rl.model.RLModuleInsight;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Repository for RL module insights
 */
@Repository
public interface RLModuleInsightRepository extends MongoRepository<RLModuleInsight, String> {
    
    // Find by module name
    Optional<RLModuleInsight> findByModuleName(String moduleName);
    
    // Find by category
    List<RLModuleInsight> findByCategory(String category);
    
    // Find by verdict
    List<RLModuleInsight> findByVerdict(RLModuleInsight.ModuleVerdict verdict);
    
    // Find top modules by gradient importance
    List<RLModuleInsight> findTop10ByOrderByGradientImportanceDesc();
    
    // Find top correlated with reward
    @Query(value = "{}", sort = "{ 'rewardCorrelation': -1 }")
    List<RLModuleInsight> findTopByRewardCorrelation();
    
    // Find strong positive modules
    List<RLModuleInsight> findByVerdictIn(List<RLModuleInsight.ModuleVerdict> verdicts);
}
