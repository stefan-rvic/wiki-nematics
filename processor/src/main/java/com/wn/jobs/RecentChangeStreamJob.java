package com.wn.jobs;

import com.mongodb.client.model.InsertOneModel;
import com.wn.models.RecentChange;
import com.wn.operators.LogFunction;
import com.wn.operators.WindowedCounter;
import com.wn.serde.RecentChangeDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.apache.flink.connector.mongodb.sink.writer.serializer.MongoSerializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.bson.Document;

import java.time.Duration;
import java.time.LocalDateTime;

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

		MongoSink<Long> sink = MongoSink.<Long>builder()
				.setUri("mongodb://admin:adminpassword@localhost:27017/admin?authSource=admin")
				.setDatabase("wiki_changes")
				.setCollection("COUNT")
				.setSerializationSchema(
						(MongoSerializationSchema<Long>) (element, sinkContext) -> {
							Document doc = new Document()
									.append("timestamp", LocalDateTime.now().toString())
									.append("count", element)
									.append("window_size_seconds", 10);

							return new InsertOneModel<>(doc.toBsonDocument());})
				.build();

		env
				.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
				.map(new LogFunction<RecentChange>().setGenerator(rc -> String.format("received changes from page : %s", rc.getMeta().getUri())))
				.keyBy(rc -> "all")
				.window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
				.apply(new WindowedCounter())
				.map(new LogFunction<Long>().setGenerator(count -> String.format("counted %d changes in 10 seconds", count)))
				.sinkTo(sink);

		env.execute("Wikipedia Change Stream Job");
	}
}
