package com.wn.serde;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.model.*;
import com.wn.models.metrics.Metric;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.mongodb.sink.config.MongoWriteOptions;
import org.apache.flink.connector.mongodb.sink.writer.context.MongoSinkContext;
import org.apache.flink.connector.mongodb.sink.writer.serializer.MongoSerializationSchema;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import java.util.logging.Level;
import java.util.logging.Logger;


public class MetricSerializer implements MongoSerializationSchema<Metric> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = Logger.getLogger(MetricSerializer.class.getName());

    private transient CodecRegistry codecRegistry;

    @Override
    public void open(SerializationSchema.InitializationContext initializationContext, MongoSinkContext sinkContext, MongoWriteOptions sinkConfiguration) throws Exception {
        this.codecRegistry = CodecRegistries
                .fromRegistries(
                        MongoClientSettings.getDefaultCodecRegistry(),
                        CodecRegistries.fromProviders(
                                PojoCodecProvider.builder()
                                        .automatic(true)
                                        .register(Metric.class)
                                        .build()));
    }

    @Override
    public WriteModel<BsonDocument> serialize(Metric metric, MongoSinkContext mongoSinkContext) {
        try(BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument())){
            this.codecRegistry
                    .get(Metric.class)
                    .encode(writer, metric, EncoderContext.builder().build());

//  todo : see if upsert with merge is better than insert then aggregate
//            return new UpdateOneModel<>(
//                    Filters.eq("dt", metric.getDt()),
//                    Updates.combine(
//                            Updates.setOnInsert(writer.getDocument()),
//                            Updates.inc("count", metric.getCount())),
//                    new UpdateOptions().upsert(true));

            return new InsertOneModel<>(writer.getDocument());
        } catch (Exception e){
            LOG.log(Level.WARNING, "Could not serialize Metric");
            return null;
        }
    }
}
