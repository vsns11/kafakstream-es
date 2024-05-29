package ca.siva.kafkastream.config;

import ca.siva.kafkastream.GenericRecordUtil;
import ca.siva.kafkastream.model.AggregatedData;
import ca.siva.kafkastream.service.ElasticsearchService;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

import static ca.siva.kafkastream.GenericRecordUtil.convertValue;
import static ca.siva.kafkastream.GenericRecordUtil.findNestedValue;

@Configuration
@EnableKafkaStreams
@Slf4j
public class KafkaStreamsConfig {

    private final ElasticsearchService elasticsearchService;

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
    public Properties kafkaStreamsProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-cg-aggregator-2");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put("specific.avro.reader", specificAvroReader);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000); // Commit every 10 seconds
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10485760); // 10 MB cache
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3); // Number of stream threads
        props.put(StreamsConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 900000); // Max idle time for connections
        props.put(StreamsConfig.RECONNECT_BACKOFF_MS_CONFIG, 1000); // Reconnect backoff
        props.put(StreamsConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 10000); // Max reconnect backoff
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); // Start from latest offset
        props.put(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, 60000); // Max task idle time
        props.put("buffered.records.per.partition", 500); // Buffered records per partition
        props.put("request.timeout.ms", 15000); // Request timeout
        props.put("rocksdb.write.buffer.size", 67108864); // RocksDB write buffer size
        props.put("rocksdb.max.background.compactions", 4); // RocksDB max background compactions
        props.put("fetch.max.bytes", 10485760); // Fetch max bytes (10 MB)
        return props;
    }

    @Bean
    public KafkaStreams buildTopology(@Qualifier("customSerdeConfig") Map<String, ?> serdeConfig) {

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        final Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();
        valueGenericAvroSerde.configure(serdeConfig, false);


        KStream<String, GenericRecord> stream = streamsBuilder.stream(inputTopic, Consumed.with(Serdes.String(), valueGenericAvroSerde));

        // Split the stream into initial and other messages
        Map<String, KStream<String, GenericRecord>> branches = stream.split(Named.as("branch-"))
                .branch((key, genericRecord) -> {
                    Object nameFieldValue = convertValue(findNestedValue(genericRecord, "name"));
                    Object bpiIdFieldValue = convertValue(findNestedValue(genericRecord, "bpiId"));

                    if (nameFieldValue instanceof String name && bpiIdFieldValue instanceof String) {
                        log.info("Received event with name: {}, bpiId: {}", nameFieldValue, bpiIdFieldValue);
                        return name.equalsIgnoreCase(filterValue);
                    }
                    return false;
                }, Branched.as("firstMessage"))
                .defaultBranch(Branched.as("otherMessages"));

        KStream<String, GenericRecord> firstMessageStream = branches.get("branch-firstMessage");
        KStream<String, GenericRecord> otherMessagesStream = branches.get("branch-otherMessages");

        if (firstMessageStream == null || otherMessagesStream == null) {
            log.error("branch keys: {}", branches.keySet());
            throw new IllegalStateException("Streams not properly initialized");
        }

        firstMessageStream.peek((key, value) -> {
            AggregatedData aggregatedData = AggregatedData.builder()
                    .id((String) convertValue(findNestedValue(value, "bpiId")))
                    .relatedEvents(new ArrayList<>())
                    .build();
            aggregatedData.add(GenericRecordUtil.convertGenericRecordToMap(value));
            log.info("Ingesting initial event to Elasticsearch: key = {}, value = {}", key, aggregatedData);

            elasticsearchService.ingestToEs(aggregatedData);
        });

        // Create a KTable from the firstMessageStream
        KStream<String, GenericRecord> rekeyedFirstMessagesStream = firstMessageStream
                .selectKey((key, value) -> (String) convertValue(findNestedValue(value, "bpiId")));

        // Rekey other messages by bpiId
        KStream<String, GenericRecord> rekeyedOtherMessagesStream = otherMessagesStream
                .selectKey((key, value) -> (String) convertValue(findNestedValue(value, "bpiId")));

        // Create a windowed join between the streams
        Duration joinWindowDuration = Duration.ofMinutes(40);
        KStream<String, AggregatedData> joinedStream = rekeyedOtherMessagesStream
                .join(
                        rekeyedFirstMessagesStream.selectKey((key, value) -> (String) convertValue(findNestedValue(value, "bpiId"))),
                        (otherMessage, firstMessage) -> {
                            log.info("Joining otherMessage: {}, with firstMessage: {}", otherMessage, firstMessage);
                            AggregatedData aggregatedData = AggregatedData.builder()
                                    .id((String) convertValue(findNestedValue(firstMessage, "bpiId")))
                                    .latestUpdateTimestamp((String) convertValue(findNestedValue(otherMessage, "timestamp")))
                                    .relatedEvents(new ArrayList<>())
                                    .build();
                            aggregatedData.add(GenericRecordUtil.convertGenericRecordToMap(otherMessage));
                            return aggregatedData;
                        },
                        JoinWindows.ofTimeDifferenceAndGrace(joinWindowDuration, Duration.ofMinutes(5)),
                        StreamJoined.with(Serdes.String(), valueGenericAvroSerde, valueGenericAvroSerde)
                );


        // Process the joined stream and push the subsequent events to Elasticsearch
        joinedStream.peek((key, value) -> {
            log.info("Ingesting subsequent event to Elasticsearch: key = {}, value = {}", key, value);
            elasticsearchService.ingestToEs(value);
        });
        final KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), kafkaStreamsProperties());
        streams.start();

        return streams;
    }
}
