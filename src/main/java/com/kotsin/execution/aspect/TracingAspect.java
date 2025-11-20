package com.kotsin.execution.aspect;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

/**
 * 🔍 Tracing Aspect for Critical Operations
 * 
 * Automatically logs entry/exit with timing for critical methods
 * Works with Spring Cloud Sleuth trace IDs
 */
@Aspect
@Component
@Slf4j
public class TracingAspect {
    
    /**
     * Trace all order placement methods
     */
    @Around("execution(* com.kotsin.execution.service.*.placeOrder(..))")
    public Object traceOrderPlacement(ProceedingJoinPoint joinPoint) throws Throwable {
        long start = System.currentTimeMillis();
        Object[] args = joinPoint.getArgs();
        
        log.info("🔍 [TRACE_ORDER_START] method={} args={}", 
                joinPoint.getSignature().getName(), 
                args.length > 0 ? args[0] : "none");
        
        try {
            Object result = joinPoint.proceed();
            long duration = System.currentTimeMillis() - start;
            
            log.info("🔍 [TRACE_ORDER_END] method={} duration={}ms result={}", 
                    joinPoint.getSignature().getName(),
                    duration,
                    result);
            
            return result;
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - start;
            log.error("🔍 [TRACE_ORDER_ERROR] method={} duration={}ms error={}", 
                    joinPoint.getSignature().getName(),
                    duration,
                    e.getMessage());
            throw e;
        }
    }
    
    /**
     * Trace all signal generation methods
     */
    @Around("execution(* com.kotsin.strategy.service.*.generateSignal(..))")
    public Object traceSignalGeneration(ProceedingJoinPoint joinPoint) throws Throwable {
        long start = System.currentTimeMillis();
        
        log.info("🔍 [TRACE_SIGNAL_START] method={}", joinPoint.getSignature().getName());
        
        try {
            Object result = joinPoint.proceed();
            long duration = System.currentTimeMillis() - start;
            
            log.info("🔍 [TRACE_SIGNAL_END] method={} duration={}ms hasSignal={}", 
                    joinPoint.getSignature().getName(),
                    duration,
                    result != null);
            
            return result;
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - start;
            log.error("🔍 [TRACE_SIGNAL_ERROR] method={} duration={}ms error={}", 
                    joinPoint.getSignature().getName(),
                    duration,
                    e.getMessage());
            throw e;
        }
    }
    
    /**
     * Trace broker API calls
     */
    @Around("execution(* com.kotsin.execution.service.FivePaisaBrokerService.*(..))")
    public Object traceBrokerAPI(ProceedingJoinPoint joinPoint) throws Throwable {
        long start = System.currentTimeMillis();
        String methodName = joinPoint.getSignature().getName();
        
        log.debug("🔍 [TRACE_BROKER_API] method={} start", methodName);
        
        try {
            Object result = joinPoint.proceed();
            long duration = System.currentTimeMillis() - start;
            
            log.info("🔍 [TRACE_BROKER_API] method={} duration={}ms success=true", 
                    methodName, duration);
            
            // Alert if broker API is slow (>500ms)
            if (duration > 500) {
                log.warn("⚠️ [TRACE_BROKER_SLOW] method={} took {}ms (threshold: 500ms)", 
                        methodName, duration);
            }
            
            return result;
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - start;
            log.error("🔍 [TRACE_BROKER_API] method={} duration={}ms error={}", 
                    methodName, duration, e.getMessage());
            throw e;
        }
    }
}
