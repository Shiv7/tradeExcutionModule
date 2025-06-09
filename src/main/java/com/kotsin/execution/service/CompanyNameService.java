package com.kotsin.execution.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service for mapping script codes to company names using existing FNO API infrastructure
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class CompanyNameService {
    
    private final RestTemplate restTemplate = new RestTemplate();
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    @Value("${company.mapping.api.base.url:http://13.126.14.119:8102}")
    private String scripFinderApiUrl;
    
    // Cache for company name mappings
    private final Map<String, String> companyNameCache = new ConcurrentHashMap<>();
    
    // Static mappings for common script codes (fallback)
    private final Map<String, String> staticMappings = new HashMap<>();
    
    @PostConstruct
    public void initializeStaticMappings() {
        // Add common known mappings as fallback
        staticMappings.put("4204", "MOTHERSON");
        staticMappings.put("53213", "RELIANCE");
        staticMappings.put("134082", "TATA MOTORS");
        staticMappings.put("105379", "HDFC BANK");
        staticMappings.put("10243", "INFOSYS");
        staticMappings.put("145997", "SBI");
        staticMappings.put("114311", "WIPRO");
        staticMappings.put("112325", "TCS");
        staticMappings.put("31181", "ITC");
        staticMappings.put("132218", "AXIS BANK");
        staticMappings.put("57275", "ICICI BANK");
        staticMappings.put("117637", "MARUTI");
        staticMappings.put("57254", "BAJAJ FINANCE");
        staticMappings.put("128480", "L&T");
        staticMappings.put("999920041", "ADANI PORTS");
        staticMappings.put("3718", "BHARTI AIRTEL");
        staticMappings.put("57057", "TITAN");
        staticMappings.put("57201", "ASIAN PAINTS");
        staticMappings.put("145996", "KOTAK BANK");
        staticMappings.put("113012", "ULTRATECH CEMENT");
        staticMappings.put("53479", "NESTLE");
        staticMappings.put("19234", "HCL TECH");
        
        log.info("🏢 Initialized {} static company name mappings", staticMappings.size());
        log.info("🔗 ScripFinder API configured at: {}", scripFinderApiUrl);
        
        // Try to refresh from API on startup
        try {
            refreshCompanyMappingsFromFnoApi();
        } catch (Exception e) {
            log.warn("⚠️ Could not fetch company mappings from FNO API on startup: {}", e.getMessage());
        }
    }
    
    /**
     * Get company name for script code
     */
    public String getCompanyName(String scripCode) {
        if (scripCode == null || scripCode.trim().isEmpty()) {
            return "Unknown";
        }
        
        scripCode = scripCode.trim();
        
        // Check cache first
        if (companyNameCache.containsKey(scripCode)) {
            return companyNameCache.get(scripCode);
        }
        
        // Try static mappings
        if (staticMappings.containsKey(scripCode)) {
            String companyName = staticMappings.get(scripCode);
            companyNameCache.put(scripCode, companyName);
            return companyName;
        }
        
        // Try API call (if available)
        try {
            refreshCompanyMappingsFromFnoApi();
            if (companyNameCache.containsKey(scripCode)) {
                return companyNameCache.get(scripCode);
            }
        } catch (Exception e) {
            log.warn("⚠️ Could not fetch company mappings from FNO API: {}", e.getMessage());
        }
        
        // Fallback to generic name
        String fallbackName = "Company_" + scripCode;
        companyNameCache.put(scripCode, fallbackName);
        return fallbackName;
    }
    
    /**
     * Refresh company mappings from FNO API using the same endpoint as other modules
     */
    public void refreshCompanyMappingsFromFnoApi() {
        try {
            log.info("🔄 Refreshing company name mappings from ScripFinder API...");
            
            String apiUrl = scripFinderApiUrl + "/getAllNiftyGroupsData?requestId=tradeExecution";
            log.debug("📡 Calling ScripFinder API: {}", apiUrl);
            
            // Call the same API that other modules use
            @SuppressWarnings("unchecked")
            Map<String, Object> response = restTemplate.getForObject(apiUrl, Map.class);
            
            if (response != null && response.containsKey("response")) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> fnoList = (List<Map<String, Object>>) response.get("response");
                
                int mappingsAdded = 0;
                for (Map<String, Object> fnoItem : fnoList) {
                    String scripCode = (String) fnoItem.get("scripCode");
                    String companyName = (String) fnoItem.get("companyName");
                    String fullName = (String) fnoItem.get("fullName");
                    String symbol = (String) fnoItem.get("symbol");
                    
                    if (scripCode != null && !scripCode.trim().isEmpty()) {
                        // Prefer fullName, then companyName, then symbol
                        String displayName = fullName;
                        if (displayName == null || displayName.trim().isEmpty()) {
                            displayName = companyName;
                        }
                        if (displayName == null || displayName.trim().isEmpty()) {
                            displayName = symbol;
                        }
                        if (displayName == null || displayName.trim().isEmpty()) {
                            displayName = "Company_" + scripCode;
                        }
                        
                        companyNameCache.put(scripCode.trim(), displayName.trim().toUpperCase());
                        mappingsAdded++;
                    }
                }
                
                log.info("✅ Successfully loaded {} company mappings from ScripFinder API (port 8102)", mappingsAdded);
            } else {
                log.warn("⚠️ Empty or invalid response from ScripFinder API");
            }
            
        } catch (Exception e) {
            log.error("🚨 Failed to refresh company mappings from ScripFinder API: {}", e.getMessage());
            log.error("🔍 API URL was: {}/getAllNiftyGroupsData?requestId=tradeExecution", scripFinderApiUrl);
        }
    }
    
    /**
     * Get all cached mappings
     */
    public Map<String, String> getAllMappings() {
        Map<String, String> allMappings = new HashMap<>(staticMappings);
        allMappings.putAll(companyNameCache);
        return allMappings;
    }
    
    /**
     * Add custom mapping
     */
    public void addMapping(String scripCode, String companyName) {
        if (scripCode != null && companyName != null) {
            companyNameCache.put(scripCode.trim(), companyName.trim());
            log.info("📝 Added custom mapping: {} -> {}", scripCode, companyName);
        }
    }
    
    /**
     * Get mapping statistics
     */
    public Map<String, Object> getMappingStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalMappings", companyNameCache.size() + staticMappings.size());
        stats.put("cachedMappings", companyNameCache.size());
        stats.put("staticMappings", staticMappings.size());
        stats.put("scripFinderApiUrl", scripFinderApiUrl + "/getAllNiftyGroupsData");
        stats.put("apiPort", "8102");
        stats.put("apiModule", "ScripFinder");
        return stats;
    }
    
    /**
     * Clear cache and reload from API
     */
    public void refreshCache() {
        companyNameCache.clear();
        refreshCompanyMappingsFromFnoApi();
        log.info("🔄 Company name cache refreshed from ScripFinder API. Total mappings: {}", 
                companyNameCache.size() + staticMappings.size());
    }
} 