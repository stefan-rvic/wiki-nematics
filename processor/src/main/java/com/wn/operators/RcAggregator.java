package com.wn.operators;

import com.wn.models.RecentChange;
import com.wn.models.metrics.Metric;
import lombok.Data;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

// todo : imp more metrics
public class RcAggregator implements AggregateFunction<RecentChange, RcAggregator.Accumulator, Metric> {
    private static final long serialVersionUID = 1L;

    @Data
    public static class Accumulator{
        private int count;
        private Map<String, Integer> domainCount;


        public Accumulator addDomainCount(String key){
            domainCount.put(key, domainCount.getOrDefault(key,0) + 1);
            return this;
        }

        public Accumulator mergeDomainCount(Map<String, Integer> domainCount2){
            domainCount2.forEach((key, value)->domainCount.merge(key, value, Integer::sum));
            return this;
        }
    }

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator()
                .setCount(0)
                .setDomainCount(new HashMap<>());
    }

    @Override
    public Accumulator add(RecentChange recentChange, Accumulator accumulator) {
        return accumulator
                .setCount(accumulator.getCount() + 1)
                .addDomainCount(recentChange.getWiki());
    }

    @Override
    public Metric getResult(Accumulator accumulator) {
        return new Metric()
                .setCount(accumulator.getCount())
                .setDomainCount(accumulator.getDomainCount());
    }

    @Override
    public Accumulator merge(Accumulator accumulator, Accumulator acc1) {
        return accumulator
                .setCount(accumulator.getCount() + acc1.getCount())
                .mergeDomainCount(accumulator.getDomainCount());
    }

    public static class ResultFunction implements WindowFunction<Metric, Metric, Instant, TimeWindow>{
        private static final long serialVersionUID = 1L;

        @Override
        public void apply(Instant instant, TimeWindow timeWindow, Iterable<Metric> iterable, Collector<Metric> collector) throws Exception {
            collector.collect(iterable.iterator().next().setDt(instant));
        }
    }
}
