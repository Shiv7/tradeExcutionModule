package com.kotsin.execution.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ErrorMonitoringService {

    public void recordError(String area, String message, Throwable t) {
        log.error("Error area={} msg={}", area, message, t);
    }

    public void recordWarn(String area, String message) {
        log.warn("Warn area={} msg={}", area, message);
    }
}
