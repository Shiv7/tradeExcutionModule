package com.kotsin.execution.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class HistoricalDataResponse {
    @JsonProperty("Datetime")
    private String datetime;
    @JsonProperty("Open")
    private double open;
    @JsonProperty("High")
    private double high;
    @JsonProperty("Low")
    private double low;
    @JsonProperty("Close")
    private double close;
    @JsonProperty("Volume")
    private long volume;
}
