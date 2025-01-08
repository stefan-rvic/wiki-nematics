package com.wn.jobs;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import com.wn.models.RecentChange;
import com.wn.models.metrics.Metric;
import com.wn.operators.CanaryFilter;
import com.wn.operators.LogFunction;
import com.wn.operators.RcAggregator;
import com.wn.serde.MetricSerializer;
import com.wn.serde.RecentChangeDeserializer;

public class RecentChangeStreamJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties kafkaProps = getProperties();

        KafkaSource<RecentChange> source = KafkaSource.<RecentChange>builder()
                .setBootstrapServers("bootstrap.wiki-nematics.europe-west9.managedkafka.wiki-nematics.cloud.goog:9092")
                .setTopics("wikipedia-changes")
                .setGroupId("flink-consumer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new RecentChangeDeserializer())
                .setProperties(kafkaProps)
                .build();

        MongoSink<Metric> sink = MongoSink.<Metric>builder()
                .setUri("mongodb+srv://michelgrolet:OGmqnijL5ZYjHMRa@wiki-nematics.55umw.mongodb.net")
                .setDatabase("wiki-nematics")
                .setCollection("CHANGES")
                .setSerializationSchema(new MetricSerializer())
                .build();

        long time = 10L;
        env
                .fromSource(
                        source,
                        WatermarkStrategy
                                .<RecentChange>forBoundedOutOfOrderness(Duration.ofSeconds(time))
                                .withTimestampAssigner(
                                        (SerializableTimestampAssigner<RecentChange>) (rc, l) -> rc.getTimestamp().toEpochMilli())
                                .withIdleness(Duration.ofSeconds(65L)),
                        "Kafka Source")
                .map(new LogFunction<RecentChange>()
                        .setFormatter(rc -> String.format("received changes from page dated at : %s", rc.getTimestamp().toString())))
                .filter(new CanaryFilter())
                .keyBy(rc -> rc
                .getTimestamp()
                .truncatedTo(ChronoUnit.MINUTES))
                .window(TumblingEventTimeWindows
                        .of(Time.seconds(time)))
                .aggregate(
                        new RcAggregator(),
                        new RcAggregator.ResultFunction())
                .map(new LogFunction<Metric>()
                        .setFormatter(m -> String.format("flink processed %d changes for this time %s", m.getCount(), m.getDt().toString())))
                .sinkTo(sink);

        env.execute("Wikipedia Change Stream Job");
    }

    private static Properties getProperties() {
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("security.protocol", SecurityProtocol.SASL_SSL.name);
        kafkaProps.setProperty("sasl.mechanism", "PLAIN");
        kafkaProps.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required "+
                "username=\"615698213919-compute@developer.gserviceaccount.com\" " + "password=\"ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsCiAgInByb2plY3RfaWQiOiAid2lraS1uZW1hdGljcyIsCiAgInByaXZhdGVfa2V5X2lkIjogImRhYjZjNjY0NDYwYzUzZGUxYzQ2OTE2NDlmOGVjMDZmYTk5YTkyYmYiLAogICJwcml2YXRlX2tleSI6ICItLS0tLUJFR0lOIFBSSVZBVEUgS0VZLS0tLS1cbk1JSUV2Z0lCQURBTkJna3Foa2lHOXcwQkFRRUZBQVNDQktnd2dnU2tBZ0VBQW9JQkFRQzZ5MW9BUUpnaHQrOUZcbkd0MGFQQkprN1hGUXRSMTVXUE5BNENHQ0FPRUFheVZwMGNXcE1sNzhWUzRTRkpyNlNIQVRNdDgxL2NsSEZaUlRcblBPVjBTMWlVc25VTWY4elZmVWF4T3k4Vmd4bWgrcWlsd2xaQ2ZnTXhVaDFySlN0ejRDUnM5T3FUMWZ3NVZXVWtcbjg4cXpCRVFvR3E2cElhb2NhcGVTTWNVdGM0QUppclpXTldVZ1l1NTJqQUx6ZEtiam9rT0d0MHVVNXl5TXVWdjFcbm5EMjZlYkp4Uk91MGRiaUsxYllPeHRObS9pTGpWeWpmaFB0bllsZGtXMjFrazkxQ2ZIdUs2VUo5d0dSMDh0Q1dcbjRTL0lORlRCTHFnVTVqYURobVB1SW5COUNGeFByNGRrdU1xZ3cyYytyTWp1OE9PRm1ZVW8yYml6M0JpVXZqRDZcbnBIUG9aUHAzQWdNQkFBRUNnZ0VBQnhxS3FWaDJNS1Mvd0Y5ZGZ6ZVU5TlQ1ZitTVFlyVURuZU9vTFRzcFUyRy9cbndpbDNISkIzZjUzTVZic0dOY25NRy83Tm5FNWRlWXJwakNJcHU3ck4wSmN2ZWwwMTIxSHdxVXdqTzBIZXdnUGlcbnBxM0svVlpiZXJteERXSnBmUFpPTnYwNEdvMHR2V1J3L0puRTRLVk40NVNOaGVNV01XUjFyOXFkWEFyT0dMbG1cbmtkclgwMHU0R0MvU1RxQWRrWDd2VHZBMGk5MHpreVhqNGgrNWE4Y1NJL0UxbENKVUR4NVp4NitzaHZwSXUzUkhcbi92S2h3UkZFUTJXdVBYN0p4MW9FY2FIVDZYdWdXV1VMbUwrV1JMbmEydnBJYm5hWGJXV0RaMTBPM0tQc3laRHZcbkN6VU9CaWNmZHhZUmpBWjZXSUliOEtRYnMzUVpTUUtOeWhiM2xUQzB1UUtCZ1FEL0hEVkdHNE9TeFZ2VE9WUkZcbjljVVh4SWZ4Sjl3Ums4OFJ6ck5ZZnVBQUNBNm0wTE1LNkNCeEJZWUE0MEhQSnQ5UkpWZVQzN0FxaHRuWnY4b0JcbmNsWHpIUFVtaytrQW1MOW9oOVpqaTZZdTg5bmhUUGFqazl6WVdjMXRXbWdjcnY0S3dLZzVvMm9RQk5EVC9KTGFcbjYra1RqbmdkV3d1LzdrNEN3QWk4YXUyK1ZRS0JnUUM3Y2lTbTdsSnRaeXh3SGlDaDlCUHdLYUo3TDFack9hNDdcbmZ4MGZGQW53Uno3eVJZVThIN1JTU0ZlUm9tNjlGSWNaMmlqekpvOUpZdFlDN3pnL3hrK1NTcnFJWWY4eE9ERmpcbkprNHRWcXFieFRQVTJFUGhSbE5haDA3dHpUQmgyWTY5cFIyd3JFcXhGSVRTM3JaREMrNFlYby9jcG5QbE9MaUxcbitkSHpIZnJKbXdLQmdRQzQzVFVSdDdCeHRFaWxXTjdqejRSaWc4MUxDT3BsWm1uZ2FwdjJIZ0t1b3lnUzVCdEtcblpRblZQUDV0T0VHaEhuY25jMXJ5VWw5emdjTHVFeGdNVWxGTVdnaWdTd0RHcU9uVGt0UGQwUDI4K29KQnpLYlJcbnhMMTluaDNLQjRCNGdLcWhHaGtObzRpaFVRd1BBZkZkYVNTK1FqaHlkVjZmVjgzNkdqUjZiVFlZL1FLQmdHblpcbk92bEkreUxzY0J1ZjU2Mk10dldYalRraXNobzZxRGpRdnhFZHI3OFBmR3d5OWRuTnpYWHBoQW1wUC85bDZDU2hcbkhSNnhWNWlKUjNEQXhYSzkrWkVTd2VMaDg4bEhnaHdMTlhwRXhuTFFHVVRJR3d6TE9hYVZZZXpIUWRyL2o2dG5cbjRpd3lIcnVBYXNEcEl1TVppWW9aWEFPdHV5UmxzMURYOGNibjF3YWhBb0dCQUorQjhuK0Z1dFZqdGY0NEdYUCtcbmdQaWFDMW5RV2FFMVVxTEdkK3E0eWljN1F4N1ppYVh3K2Fia0VwOFI1YlRIdFN0bDhyTFlCbUFDOENPcWcySHNcbnNCeDZFYW1LaHo2VmdJNWhDZWhyTHd1d0YrWkxENnlFWnNPRks5ZHRuVlBBcXZROEpReHpjUHRBNlAzZkQ0YTZcbmtJNUFydUVTSml6UnhaL3RZWndnREVUUVxuLS0tLS1FTkQgUFJJVkFURSBLRVktLS0tLVxuIiwKICAiY2xpZW50X2VtYWlsIjogIjYxNTY5ODIxMzkxOS1jb21wdXRlQGRldmVsb3Blci5nc2VydmljZWFjY291bnQuY29tIiwKICAiY2xpZW50X2lkIjogIjExNjEyMjc1Njk2NzQ3NTAzODYzNCIsCiAgImF1dGhfdXJpIjogImh0dHBzOi8vYWNjb3VudHMuZ29vZ2xlLmNvbS9vL29hdXRoMi9hdXRoIiwKICAidG9rZW5fdXJpIjogImh0dHBzOi8vb2F1dGgyLmdvb2dsZWFwaXMuY29tL3Rva2VuIiwKICAiYXV0aF9wcm92aWRlcl94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL29hdXRoMi92MS9jZXJ0cyIsCiAgImNsaWVudF94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL3JvYm90L3YxL21ldGFkYXRhL3g1MDkvNjE1Njk4MjEzOTE5LWNvbXB1dGUlNDBkZXZlbG9wZXIuZ3NlcnZpY2VhY2NvdW50LmNvbSIsCiAgInVuaXZlcnNlX2RvbWFpbiI6ICJnb29nbGVhcGlzLmNvbSIKfQo=\";");
        kafkaProps.setProperty("bootstrap.servers", "bootstrap.wiki-nematics.europe-west9.managedkafka.wiki-nematics.cloud.goog:9092");
        return kafkaProps;
    }
}
