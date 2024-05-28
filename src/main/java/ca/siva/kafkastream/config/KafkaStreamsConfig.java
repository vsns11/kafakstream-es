package ca.siva.kafkastream.config;

import ca.siva.kafkastream.GenericRecordUtil;
import ca.siva.kafkastream.model.AggregatedData;
import ca.siva.kafkastream.processor.SuppressWindowProcessorSupplier;
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
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.*;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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

    @Value("${spring.kafka.streams.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Value("${schema.specific.avro.reader}")
    private String specificAvroReader;

    public KafkaStreamsConfig(final ElasticsearchService elasticsearchService) {
        this.elasticsearchService = elasticsearchService;
    }

    @Bean(name = "customSerdeConfig")
    public Map<String, ?> serdeConfig() {
        return Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl, "specific.avro.reader", specificAvroReader);
    }

    @Bean
    public KStream<String, GenericRecord> kStream(StreamsBuilder streamsBuilder, @Qualifier("customSerdeConfig") Map<String, ?> serdeConfig) {
        final Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();
        log.info("map: {}", serdeConfig);
        valueGenericAvroSerde.configure(serdeConfig, false);
        Serde<AggregatedData> aggregatedJsonMessageSerde = new JsonSerde<>(AggregatedData.class);

        // Define the state store
//        StoreBuilder<SessionStore<String, AggregatedData>> storeBuilder = Stores.sessionStoreBuilder(
//                Stores.persistentSessionStore("aggregated-window-store", Duration.ofMinutes(5)),
//                Serdes.String(),
//                aggregatedJsonMessageSerde
//        );
//        streamsBuilder.addStateStore(storeBuilder);

        KStream<String, GenericRecord> stream = streamsBuilder.stream(inputTopic, Consumed.with(Serdes.String(), valueGenericAvroSerde));

        Named named = Named.as("branch");
        Map<String, KStream<String, GenericRecord>> branches = stream.split(named).branch((key, genericRecord) -> {
            Object nameFieldValue = convertValue(findNestedValue(genericRecord, "name"));
            Object bpiIdFieldValue = convertValue(findNestedValue(genericRecord, "bpiId"));

            log.info("name read: {}, type: {}", nameFieldValue, nameFieldValue.getClass().getName());
            log.info("bpiId read: {}, type: {}", bpiIdFieldValue, bpiIdFieldValue.getClass().getName());

            if (nameFieldValue instanceof String name && bpiIdFieldValue instanceof String) {
                log.info("First message Data: {}, name: {}, bpiId: {}", genericRecord, name, bpiIdFieldValue);
                return name.equalsIgnoreCase(filterValue);
            }
            return false;
        }, Branched.as("firstMessage")).defaultBranch(Branched.as("otherMessages"));

        KStream<String, GenericRecord> firstMessageStream = branches.get("branchfirstMessage");
        KStream<String, GenericRecord> otherMessagesStream = branches.get("branchotherMessages");

        if (firstMessageStream == null || otherMessagesStream == null) {
            log.error("branch keys :{}", branches.keySet());
            throw new IllegalStateException("Streams not properly initialized");
        }

        KTable<String, GenericRecord> firstMessageTable = firstMessageStream.selectKey((String key, GenericRecord value) -> {
            String newKey = (String) convertValue(findNestedValue(value, "bpiId"));
            log.info("selectKey for firstMessageTable: key = {}, newKey = {}", key, newKey);
            return newKey;
        }).toTable(Materialized.with(Serdes.String(), valueGenericAvroSerde));

        KStream<String, GenericRecord> rekeyedOtherMessagesStream = otherMessagesStream.selectKey((String key, GenericRecord value) -> {
            String newKey = (String) convertValue(findNestedValue(value, "bpiId"));
            log.info("selectKey for otherMessagesStream: key = {}, newKey = {}", key, newKey);
            return newKey;
        });

        KStream<String, Map<String, Object>> joinedStream = rekeyedOtherMessagesStream.join(firstMessageTable, (otherMessage, firstMessage) -> {
            log.info("Processing for joinedStream: otherMessage = {}, firstMessage = {}", otherMessage, firstMessage);
            Map<String, Object> combinedMap = new HashMap<>();
            combinedMap.put("initialEvent", GenericRecordUtil.convertGenericRecordToMap(firstMessage));
            combinedMap.put("receivedEvent", GenericRecordUtil.convertGenericRecordToMap(otherMessage));
            return combinedMap;
        });

        KTable<Windowed<String>, AggregatedData> aggregatedTable = joinedStream
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>()))
                .windowedBy(SessionWindows.ofInactivityGapAndGrace(Duration.ofSeconds(30), Duration.ofSeconds(10)))
                .aggregate(AggregatedData::new, (key, value, aggregate) -> {
            log.info("Aggregating key: {}, value: {}", key, value);
            if (!aggregate.isStartEventAdded()) {
                @SuppressWarnings("unchecked") Map<String, Object> firstMessageMap = (Map<String, Object>) value.get("initialEvent");
                aggregate.add(firstMessageMap);
                aggregate.setStartEventAdded(true);
            }
            aggregate.add((Map<String, Object>) value.get("receivedEvent"));
            return aggregate;
        }, (aggKey, aggOne, aggTwo) -> {
            log.info("Merging aggregates: aggOne = {}, aggTwo = {}", aggOne, aggTwo);
            aggOne.merge(aggTwo);
            return aggOne;
        }, Materialized.<String, AggregatedData, SessionStore<Bytes, byte[]>>as("aggregated-window-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(aggregatedJsonMessageSerde))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));

        aggregatedTable.toStream()
                .process(new SuppressWindowProcessorSupplier(elasticsearchService), "aggregated-window-store");
        return stream;
    }
}
