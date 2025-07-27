package com.kotsin.execution.model;

import lombok.Data;
import java.util.Map;

@Data
public class PivotData {
    private double pivot;
    private double s1;
    private double s2;
    private double s3;
    private double s4;
    private double r1;
    private double r2;
    private double r3;
    private double r4;

    @SuppressWarnings("unchecked")
    public static PivotData fromMap(Map<String, Object> map) {
        PivotData data = new PivotData();
        data.setPivot(((Number) map.get("pivot")).doubleValue());
        data.setS1(((Number) map.get("s1")).doubleValue());
        data.setS2(((Number) map.get("s2")).doubleValue());
        data.setS3(((Number) map.get("s3")).doubleValue());
        data.setS4(((Number) map.get("s4")).doubleValue());
        data.setR1(((Number) map.get("r1")).doubleValue());
        data.setR2(((Number) map.get("r2")).doubleValue());
        data.setR3(((Number) map.get("r3")).doubleValue());
        data.setR4(((Number) map.get("r4")).doubleValue());
        return data;
    }
}
