package com.wn.serde;

import com.mongodb.client.model.*;
import com.wn.models.metrics.Metric;
import org.apache.flink.connector.mongodb.sink.writer.context.MongoSinkContext;
import org.apache.flink.connector.mongodb.sink.writer.serializer.MongoSerializationSchema;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;


public class MetricSerializer implements MongoSerializationSchema<Metric> {
    private static final long serialVersionUID = 1L;

    @Override
    public WriteModel<BsonDocument> serialize(Metric metric, MongoSinkContext mongoSinkContext) {
        return new UpdateOneModel<>(
                Filters.eq("dt", metric.getDt()),
                Updates.combine(
                        Updates.inc("count", metric.getCount()),
                        Updates.combine(
                                metric
                                        .getDomainCount()
                                        .entrySet()
                                        .stream()
                                        .map(entry ->
                                                Updates.inc(
                                                        "domainCount." + entry.getKey().replace(".", "_"),
                                                        entry.getValue()))
                                        .toArray(Bson[]::new)
                        ),
                        Updates.inc("bytesChangedCount", metric.getBytesChangedCount()),
                        Updates.inc("changeCountByBots", metric.getChangeCountByBots()),
                        Updates.inc("changeCountByHumans", metric.getChangeCountByHumans())
                ),
                new UpdateOptions().upsert(true));
    }
}
