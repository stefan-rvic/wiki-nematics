package com.wn.jobs;

import com.mongodb.client.model.InsertOneModel;
import com.wn.serde.DocumentDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.apache.flink.connector.mongodb.sink.writer.serializer.MongoSerializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.bson.BsonDocument;

public class StreamJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<BsonDocument> source = KafkaSource.<BsonDocument>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("wikipedia-changes")
                .setGroupId("flink-consumer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new DocumentDeserializer())
                .build();

        MongoSink<BsonDocument> sink = MongoSink.<BsonDocument>builder()
                .setUri("mongodb://admin:adminpassword@localhost:27017/admin?authSource=admin")
                .setDatabase("wiki_stream")
                .setCollection("CHANGES")
                .setSerializationSchema(
                        (MongoSerializationSchema<BsonDocument>) (element, sinkContext) -> new InsertOneModel<>(element))
                .build();

        env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .sinkTo(sink);

        env.execute("Wikipedia Change Stream Job Into Time Series Collection");
    }
}

