package com.wn.operators;

import com.wn.models.RecentChange;
import com.wn.models.metrics.Metric;
import lombok.Data;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;

// todo : imp more metrics
public class RcAggregator implements AggregateFunction<RecentChange, RcAggregator.Accumulator, Metric> {
    private static final long serialVersionUID = 1L;

    @Data
    public static class Accumulator{
        private int count;
    }

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator()
                .setCount(0);
    }

    @Override
    public Accumulator add(RecentChange recentChange, Accumulator accumulator) {
        return accumulator
                .setCount(accumulator.getCount() + 1);
    }

    @Override
    public Metric getResult(Accumulator accumulator) {
        return new Metric()
                .setCount(accumulator.getCount());
    }

    @Override
    public Accumulator merge(Accumulator accumulator, Accumulator acc1) {
        return accumulator
                .setCount(accumulator.getCount() + acc1.getCount());
    }

    public static class ResultFunction implements WindowFunction<Metric, Metric, Instant, TimeWindow>{
        private static final long serialVersionUID = 1L;

        @Override
        public void apply(Instant instant, TimeWindow timeWindow, Iterable<Metric> iterable, Collector<Metric> collector) throws Exception {
            collector.collect(iterable.iterator().next().setDt(instant));
        }
    }
}
