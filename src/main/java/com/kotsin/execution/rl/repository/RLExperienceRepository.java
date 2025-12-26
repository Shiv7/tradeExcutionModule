package com.kotsin.execution.rl.repository;

import com.kotsin.execution.rl.model.RLExperience;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Repository for RL experiences (training data)
 */
@Repository
public interface RLExperienceRepository extends MongoRepository<RLExperience, String> {
    
    // Find by source
    List<RLExperience> findBySource(RLExperience.ExperienceSource source);
    
    // Find recent experiences for training
    List<RLExperience> findTop1000ByOrderByCreatedAtDesc();
    
    // Count by source
    long countBySource(RLExperience.ExperienceSource source);
    
    // Find winning experiences
    List<RLExperience> findByWonTrue();
    
    // Find by signal type
    List<RLExperience> findBySignalType(String signalType);
    
    // Find experiences after a timestamp
    List<RLExperience> findByCreatedAtAfter(LocalDateTime timestamp);
    
    // Count all
    long count();
    
    // Find by rMultiple range
    @Query("{ 'rMultiple': { $gte: ?0, $lte: ?1 } }")
    List<RLExperience> findByRMultipleBetween(double min, double max);
    
    // Find experiences for weighted sampling
    @Query(value = "{}", sort = "{ 'sampleWeight': -1, 'createdAt': -1 }")
    List<RLExperience> findTopByWeightedOrder(int limit);
}
