package com.wn.operators;

import com.wn.models.RecentChange;
import com.wn.models.metrics.Metric;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.HashMap;

public class RcAggregator implements AggregateFunction<RecentChange, Metric, Metric> {
    private static final long serialVersionUID = 1L;

    @Override
    public Metric createAccumulator() {
        return new Metric()
                .setCount(0)
                .setDomainCount(new HashMap<>())
                .setBytesChangedCount(0L)
                .setChangeCountByHumans(0)
                .setChangeCountByBots(0)
                ;
    }

    @Override
    public Metric add(RecentChange recentChange, Metric metric) {
        return metric
                .setCount(metric.getCount() + 1)
                .incDomainCount(recentChange.getServerName())
                .setBytesChangedCount(metric.getBytesChangedCount() + Math.abs(recentChange.getLength().getNewLength() - recentChange.getLength().getOldLength()))
                .setChangeCountByHumans(recentChange.isBot() ? metric.getChangeCountByHumans() : metric.getChangeCountByHumans() + 1)
                .setChangeCountByBots(recentChange.isBot() ? metric.getChangeCountByBots() + 1 : metric.getChangeCountByBots())
                ;
    }

    @Override
    public Metric getResult(Metric metric) {
        return metric;
    }

    @Override
    public Metric merge(Metric metric, Metric acc1) {
        return metric
                .setCount(metric.getCount() + acc1.getCount())
                .mergeDomainCount(metric.getDomainCount())
                .setChangeCountByBots(metric.getChangeCountByBots() + acc1.getChangeCountByBots())
                .setBytesChangedCount(metric.getBytesChangedCount() + acc1.getBytesChangedCount())
                .setChangeCountByHumans(metric.getChangeCountByHumans() + acc1.getChangeCountByHumans())
                ;
    }

    public static class ResultFunction implements WindowFunction<Metric, Metric, Instant, TimeWindow>{
        private static final long serialVersionUID = 1L;

        @Override
        public void apply(Instant instant, TimeWindow timeWindow, Iterable<Metric> iterable, Collector<Metric> collector) throws Exception {
            collector.collect(iterable.iterator().next().setDt(instant));
        }
    }
}
