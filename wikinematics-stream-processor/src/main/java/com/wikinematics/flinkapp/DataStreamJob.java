package com.wikinematics.flinkapp;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataStreamJob {

	final static String inputTopic = "input-topic";
	final static String outputTopic = "output-topic";
	final static String jobTitle = "WordCount";
 	

	public static void main(String[] args) throws Exception {
		final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		KafkaSource<String> source = KafkaSource.<String>builder()
		    .setBootstrapServers(bootstrapServers)
		    .setTopics(inputTopic)
		    .setGroupId("my-group")
		    .setStartingOffsets(OffsetsInitializer.earliest())
		    .setValueOnlyDeserializer(new SimpleStringSchema())
		    .build();

		KafkaRecordSerializationSchema<String> serializer = KafkaRecordSerializationSchema.builder()
				.setValueSerializationSchema(new SimpleStringSchema())
				.setTopic(outputTopic)
				.build();

		KafkaSink<String> sink = KafkaSink.<String>builder()
				.setBootstrapServers(bootstrapServers)
				.setRecordSerializer(serializer)
				.build();

		// Execute program, beginning computation.
		DataStream<String> text = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
		text.sinkTo(sink);
		env.execute(jobTitle);
	}
}
