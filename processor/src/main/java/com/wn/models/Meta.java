package com.wn.models;

import lombok.Data;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Meta {
    private String uri;
    private String requestId;
    private String id;
    private Instant dt;
    private String domain;
    private String stream;
    private String topic;
    private int partition;
    private long offset;
}
