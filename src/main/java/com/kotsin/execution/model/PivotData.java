package com.kotsin.execution.model;

import lombok.Data;

import java.time.LocalDate;
import java.util.Map;

@Data
public class PivotData {
    private LocalDate date;
    private String scripCode;
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
        data.setPivot(getDouble(map, "pivot"));
        data.setS1(getDouble(map, "support1"));
        data.setS2(getDouble(map, "support2"));
        data.setS3(getDouble(map, "support3"));
        data.setS4(getDouble(map, "support4"));
        data.setR1(getDouble(map, "resistance1"));
        data.setR2(getDouble(map, "resistance2"));
        data.setR3(getDouble(map, "resistance3"));
        data.setR4(getDouble(map, "resistance4"));
        return data;
    }

    private static double getDouble(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return 0.0;
    }
}
