package com.wn.models.metrics;

import lombok.Data;

import java.time.Instant;

@Data
public class Metric {
    private Instant dt;
    private int count;
}
