package com.wn.serde;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import java.io.IOException;
import java.time.Instant;

public class DocumentDeserializer implements DeserializationSchema<BsonDocument> {

    private static final long serialVersionUID = 1L;

    @Override
    public BsonDocument deserialize(byte[] bytes) throws IOException {
        BsonDocument doc = BsonDocument.parse(new String(bytes));
        return doc
                .append(
                        "dt",
                        new BsonDateTime(
                                Instant
                                        .parse(doc.getDocument("meta").getString("dt").getValue())
                                        .toEpochMilli()));
    }

    @Override
    public boolean isEndOfStream(BsonDocument bsonDocument) {
        return false;
    }

    @Override
    public TypeInformation<BsonDocument> getProducedType() {
        return TypeInformation.of(BsonDocument.class);
    }
}
