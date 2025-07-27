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
        data.setPivot(getDouble(map, "pivot"));
        data.setS1(getDouble(map, "s1"));
        data.setS2(getDouble(map, "s2"));
        data.setS3(getDouble(map, "s3"));
        data.setS4(getDouble(map, "s4"));
        data.setR1(getDouble(map, "r1"));
        data.setR2(getDouble(map, "r2"));
        data.setR3(getDouble(map, "r3"));
        data.setR4(getDouble(map, "r4"));
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
