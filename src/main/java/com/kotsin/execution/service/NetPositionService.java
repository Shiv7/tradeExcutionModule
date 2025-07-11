package com.kotsin.execution.service;

import com.kotsin.execution.broker.FivePaisaBrokerService;
import com.kotsin.execution.broker.BrokerException;
import com.kotsin.execution.model.NetPosition;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Thin wrapper around FivePaisaBrokerService to provide cached / reusable net-position access.
 * Separating this concerns keeps the consumer/controller agnostic of broker specifics.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class NetPositionService {

    private final FivePaisaBrokerService fivePaisaBrokerService;

    /** Fetches fresh positions from broker REST API. */
    public List<NetPosition> fetchAll() throws BrokerException {
        return fivePaisaBrokerService.fetchNetPositions();
    }

    /** Finds a single position matching exchange + type + scripCode (case-insensitive). */
    public NetPosition find(String exch, String exchType, String scripCode) throws BrokerException {
        return fivePaisaBrokerService.findPosition(exch, exchType, scripCode);
    }
} 