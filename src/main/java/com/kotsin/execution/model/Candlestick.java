package com.kotsin.execution.model;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Candlestick model - handles both Kafka format and 5Paisa API format
 * 
 * API Response format:
 * {
 *   "Datetime": "2025-12-26T09:15:00",
 *   "Open": 4115,
 *   "High": 4125,
 *   "Low": 4107,
 *   "Close": 4125,
 *   "Volume": 4300
 * }
 */
@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Candlestick {
    
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
    
    private String companyName;
    
    @JsonProperty("Open")
    @JsonAlias({"open"})
    private double open;
    
    @JsonProperty("High")
    @JsonAlias({"high"})
    private double high;
    
    @JsonProperty("Low")
    @JsonAlias({"low"})
    private double low;
    
    @JsonProperty("Close")
    @JsonAlias({"close"})
    private double close;
    
    @JsonProperty("Volume")
    @JsonAlias({"volume"})
    private long volume;
    
    // For internal use / Kafka format
    private long windowStartMillis;
    private long windowEndMillis;
    
    private String exchange;
    private String exchangeType;
    
    /**
     * Handle API's Datetime field and convert to windowStartMillis
     */
    @JsonProperty("Datetime")
    @JsonAlias({"datetime"})
    public void setDatetime(String datetime) {
        if (datetime != null && !datetime.isEmpty()) {
            try {
                LocalDateTime ldt = LocalDateTime.parse(datetime, FORMATTER);
                this.windowStartMillis = ldt.atZone(IST).toInstant().toEpochMilli();
                // End millis is +1 minute for 1m candles
                this.windowEndMillis = this.windowStartMillis + 60000;
            } catch (Exception e) {
                // Fallback: try to parse as epoch millis
                try {
                    this.windowStartMillis = Long.parseLong(datetime);
                } catch (NumberFormatException ignored) {}
            }
        }
    }
}
