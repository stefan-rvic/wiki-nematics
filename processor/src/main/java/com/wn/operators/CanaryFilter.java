package com.wn.operators;

import com.wn.models.RecentChange;
import org.apache.flink.api.common.functions.FilterFunction;

public class CanaryFilter implements FilterFunction<RecentChange> {
    @Override
    public boolean filter(RecentChange recentChange) {
        return !recentChange.getMeta().getDomain().equalsIgnoreCase("canary");
    }
}
