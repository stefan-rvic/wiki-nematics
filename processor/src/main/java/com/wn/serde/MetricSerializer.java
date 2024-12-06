package com.wn.serde;

import com.mongodb.client.model.*;
import com.wn.models.metrics.Metric;
import org.apache.flink.connector.mongodb.sink.writer.context.MongoSinkContext;
import org.apache.flink.connector.mongodb.sink.writer.serializer.MongoSerializationSchema;
import org.bson.BsonDocument;


public class MetricSerializer implements MongoSerializationSchema<Metric> {
    private static final long serialVersionUID = 1L;

    @Override
    public WriteModel<BsonDocument> serialize(Metric metric, MongoSinkContext mongoSinkContext) {
        return new UpdateOneModel<>(
                Filters.eq("dt", metric.getDt()),
                Updates.combine(
                        Updates.inc("count", metric.getCount())),
                new UpdateOptions().upsert(true));
    }
}
