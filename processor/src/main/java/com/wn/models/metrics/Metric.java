package com.wn.models.metrics;

import lombok.Data;

import java.time.Instant;
import java.util.Map;

@Data
public class Metric {
    private Instant dt;
    private int count;
    private Map<String, Integer> domainCount;
    private long bytesChangedCount;
    private int changeCountByHumans;
    private int changeCountByBots;

    public Metric incDomainCount(String key){
        domainCount.put(key, domainCount.getOrDefault(key,0) + 1);
        return this;
    }

    public Metric mergeDomainCount(Map<String, Integer> domainCount2){
        domainCount2.forEach((key, value) -> domainCount.merge(key, value, Integer::sum));
        return this;
    }
}
