package com.wn.models.metrics;

import lombok.Data;

import java.time.Instant;
import java.util.Map;

@Data
public class Metric {
    private Instant dt;
    private int count;
    private Map<String, Integer> domainCount;
}
