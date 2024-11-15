package com.wn;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataStreamJob {


	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


		KafkaSource<String> source = KafkaSource.<String>builder()
				.setBootstrapServers("localhost:9092")
				.setTopics("wikipedia.changes")
				.setGroupId("flink-consumer-group")
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new SimpleStringSchema())
				.build();

		DataStream<String> stream = env.fromSource(
				source,
				WatermarkStrategy.noWatermarks(),
				"Kafka Source"
		);

		stream.print();

		env.execute("Simple Kafka Consumer");
	}
}
