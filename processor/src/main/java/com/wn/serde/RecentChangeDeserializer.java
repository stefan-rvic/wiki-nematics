package com.wn.serde;

import com.wn.models.RecentChange;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.PropertyNamingStrategies;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;

public class RecentChangeDeserializer implements DeserializationSchema<RecentChange> {

    private static final long serialVersionUID = 1L;

    private transient ObjectMapper mapper;

    @Override
    public void open(InitializationContext context) throws Exception {
        mapper = new ObjectMapper()
                .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
                .registerModule(new JavaTimeModule());
    }

    @Override
    public RecentChange deserialize(byte[] bytes) throws IOException {
        return mapper.readValue(bytes, RecentChange.class);
    }

    @Override
    public boolean isEndOfStream(RecentChange recentChange) {
        return false;
    }

    @Override
    public TypeInformation<RecentChange> getProducedType() {
        return TypeInformation.of(RecentChange.class);
    }
}
