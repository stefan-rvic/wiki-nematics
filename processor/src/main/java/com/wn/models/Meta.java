package com.wn.models;

import lombok.Data;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Meta {
    private String uri;
    private String requestId;
    private String id;
    private String dt;
    private String domain;
    private String stream;
    private String topic;
    private int partition;
    private int offset;
}
