package com.wn.operators;

import com.wn.models.RecentChange;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.stream.StreamSupport;

public class WindowedCounter implements WindowFunction<RecentChange, Long, String, TimeWindow> {

    private static final long serialVersionUID = 1L;

    @Override
    public void apply(String s, TimeWindow timeWindow, Iterable<RecentChange> iterable, Collector<Long> collector) throws Exception {
        collector.collect(
                StreamSupport.stream(iterable.spliterator(), true).count()
        );
    }
}
