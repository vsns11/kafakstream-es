package ca.siva.kafkastream.config;

import ca.siva.kafkastream.GenericRecordUtil;
import ca.siva.kafkastream.model.AggregatedData;
import ca.siva.kafkastream.processor.CleanupProcessor;
import ca.siva.kafkastream.processor.CleanupProcessorSupplier;
import ca.siva.kafkastream.service.ElasticsearchService;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.SessionStore;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static ca.siva.kafkastream.GenericRecordUtil.convertValue;
import static ca.siva.kafkastream.GenericRecordUtil.findNestedValue;

@Configuration
@EnableKafkaStreams
@Slf4j
public class KafkaStreamsConfig {

    private final ElasticsearchService elasticsearchService;
    private final Duration inactivityGap = Duration.ofMinutes(2);

    @Value("${kafka.topic.input}")
    private String inputTopic;


    @Value("${message.filter}")
    private String filterValue;


    @Value("${schema.registry.url}")
    private String schemaRegistryUrl;

    @Value("${schema.specific.avro.reader}")
    private String specificAvroReader;

    public KafkaStreamsConfig(final ElasticsearchService elasticsearchService) {
        this.elasticsearchService = elasticsearchService;
    }


    @Bean(name = "customSerdeConfig")
    public Map<String, ?> serdeConfig() {
        return Map.of(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl,
                "specific.avro.reader", specificAvroReader
        );
    }

    @Bean
    public KStream<String, GenericRecord> kStream(StreamsBuilder streamsBuilder, @Qualifier("customSerdeConfig") Map<String, ?> serdeConfig) {

        final Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();
        log.info("map: {}", serdeConfig);
        valueGenericAvroSerde.configure(serdeConfig, false);
        Serde<AggregatedData> aggregatedJsonMessageSerde = new JsonSerde<>(AggregatedData.class);

        KStream< String, GenericRecord> stream = streamsBuilder
                .stream(inputTopic, Consumed.with(Serdes.String(), valueGenericAvroSerde))
                .filter((key, genericRecord) -> {
                    Object filterFieldValue =  convertValue(findNestedValue(genericRecord, "commonValue"));
                    Object groupByFieldValue =  convertValue(findNestedValue(genericRecord, "id"));

                    log.info("value read: {}, type: {}", filterFieldValue, filterFieldValue.getClass().getName());
                    log.info("groupBy read: {}, type: {}", groupByFieldValue, groupByFieldValue.getClass().getName());

                    if (filterFieldValue instanceof String commonValue && groupByFieldValue instanceof String) {
                        log.info("Data: {}, Filter: {}, Match: {}", genericRecord, filterValue, filterValue.equalsIgnoreCase(commonValue));
                        return filterValue.equalsIgnoreCase(commonValue) && Objects.nonNull(groupByFieldValue);
                    }
                    return false;
                })
                .selectKey((key, genericRecord) -> (String) convertValue(findNestedValue(genericRecord, "id")));


        KTable<Windowed<String>, AggregatedData> aggregatedTable = stream
                .groupByKey(Grouped.with(Serdes.String(), valueGenericAvroSerde))
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(inactivityGap))
                .aggregate(
                        AggregatedData::new, // Initializer
                        (key, value, aggregate) -> {
                            Map<String, Object> newValueMap = GenericRecordUtil.convertGenericRecordToMap(value);
                            aggregate.add(newValueMap);
                            return aggregate;
                        }, // Aggregator
                        (aggKey, aggOne, aggTwo) -> {
                            aggOne.merge(aggTwo);
                            return aggOne;
                        }, // Session Merger
                        Materialized.<String, AggregatedData, SessionStore<Bytes, byte[]>>as("aggregated-window-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(aggregatedJsonMessageSerde)
                )
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));

        aggregatedTable.toStream().process(() -> new CleanupProcessor(), "aggregated-window-store");

        // ingest to es
        aggregatedTable
                .toStream()
                .peek((key, value) -> log.info("event ingested: {}", value))
                .map((key, value) -> {
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                            .withZone(ZoneId.systemDefault());
                    value.setId(key.key());
                    value.setLatestUpdateTimestamp(formatter.format(Instant.ofEpochMilli(key.window().start())));
                    log.info("Aggregated value:{}", value);
                    elasticsearchService.ingestToEs(value);
                    return KeyValue.pair(key.key(), value);
                });

        // log in the console
//        aggregatedTable
//                .toStream()
//                .filter((key, value) -> value != null && !value.getMessages().isEmpty())
//                .foreach((key, value) -> {
//                    String actualKey = key.key();
//                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
//                            .withZone(ZoneId.systemDefault());
//                    String start = formatter.format(Instant.ofEpochMilli(key.window().start()));
//                    String end = formatter.format(Instant.ofEpochMilli(key.window().end()));
//                    value.setId(key.key());
//
//                    log.info("Aggregated value:{}", value);
//                });

        return stream;
    }


}


