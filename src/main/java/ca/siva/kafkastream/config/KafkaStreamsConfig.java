package ca.siva.kafkastream.config;

import ca.siva.kafkastream.GenericRecordUtil;
import ca.siva.kafkastream.model.AggregatedData;
import ca.siva.kafkastream.processor.LoggingMessagingFunction;
import ca.siva.kafkastream.service.ElasticsearchService;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.streams.messaging.MessagingFunction;
import org.springframework.kafka.streams.messaging.MessagingProcessor;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.messaging.Message;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;

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

    @Value("${spring.kafka.streams.properties.schema.registry.url}")
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
        valueGenericAvroSerde.configure(serdeConfig, false);


        KStream<String, GenericRecord> stream = streamsBuilder.stream(inputTopic, Consumed.with(Serdes.String(), valueGenericAvroSerde));

        MessagingMessageConverter messageConverter = new MessagingMessageConverter();
        MessagingFunction mf  = new LoggingMessagingFunction();

        stream.process(() -> new MessagingProcessor<>(mf, messageConverter));

        // Split the stream into initial and other messages
        Map<String, KStream<String, GenericRecord>> branches = stream.split(Named.as("branch"))
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

        KStream<String, GenericRecord> firstMessageStream = branches.get("branchfirstMessage");
        KStream<String, GenericRecord> otherMessagesStream = branches.get("branchotherMessages");

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
                        rekeyedFirstMessagesStream,
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
                        JoinWindows.ofTimeDifferenceWithNoGrace(joinWindowDuration),
                        StreamJoined.with(Serdes.String(), valueGenericAvroSerde, valueGenericAvroSerde)
                                .withThisStoreSupplier(
                                        Stores.persistentWindowStore(
                                                "joined-window-store",
                                                joinWindowDuration.plus(joinWindowDuration),
                                                joinWindowDuration.plus(joinWindowDuration),
                                                true
                                        )
                                ).withLoggingDisabled()

                );


        // Process the joined stream and push the subsequent events to Elasticsearch
        joinedStream.peek((key, value) -> {
            log.info("Ingesting subsequent event to Elasticsearch: key = {}, value = {}", key, value);
            elasticsearchService.ingestToEs(value);
        });

        return stream;
    }
}
