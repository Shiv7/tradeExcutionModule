package com.kotsin.execution.rl.repository;

import com.kotsin.execution.rl.model.RLPolicy;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Repository for RL policies
 */
@Repository
public interface RLPolicyRepository extends MongoRepository<RLPolicy, String> {
    
    // Find active policy
    Optional<RLPolicy> findByActiveTrue();
    
    // Find by version
    Optional<RLPolicy> findByVersion(String version);
    
    // Find latest by update time
    Optional<RLPolicy> findTopByOrderByLastUpdatedAtDesc();
}
