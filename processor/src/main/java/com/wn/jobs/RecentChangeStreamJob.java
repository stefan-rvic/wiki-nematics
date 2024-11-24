package com.wn.jobs;

import com.wn.models.RecentChange;
import com.wn.models.metrics.Metric;
import com.wn.operators.LogFunction;
import com.wn.operators.RcAggregator;
import com.wn.serde.MetricSerializer;
import com.wn.serde.RecentChangeDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class RecentChangeStreamJob {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		KafkaSource<RecentChange> source = KafkaSource.<RecentChange>builder()
				.setBootstrapServers("localhost:9092")
				.setTopics("wikipedia-changes")
				.setGroupId("flink-consumer-group")
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new RecentChangeDeserializer())
				.build();

		MongoSink<Metric> sink = MongoSink.<Metric>builder()
				.setUri("mongodb://admin:adminpassword@localhost:27017/admin?authSource=admin")
				.setDatabase("wiki_stream")
				.setCollection("CHANGES")
				.setSerializationSchema(new MetricSerializer())
				.build();

		env
				.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
				.map(new LogFunction<RecentChange>()
						.setGenerator(rc -> String.format("received changes from page : %s", rc.getMeta().getUri())))
				.keyBy(rc -> rc
						.getTimestamp()
						.truncatedTo(ChronoUnit.MINUTES))
				.window(TumblingProcessingTimeWindows
						.of(Duration.ofSeconds(60)))
				.aggregate(
						new RcAggregator(),
						new RcAggregator.ResultFunction())
				.map(new LogFunction<Metric>()
						.setGenerator(m -> String.format("flink processed %d changes for this time %s", m.getCount(), m.getDt().toString())))
				.sinkTo(sink);

		env.execute("Wikipedia Change Stream Job");
	}
}
