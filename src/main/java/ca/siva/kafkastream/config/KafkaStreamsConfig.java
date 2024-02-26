package ca.siva.kafkastream.config;

import ca.siva.kafkastream.model.AggregatedJsonMessage;
import ca.siva.kafkastream.model.JsonMessage;
import ca.siva.kafkastream.service.ElasticsearchService;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

@Configuration
@EnableKafkaStreams
@Slf4j
public class KafkaStreamsConfig {

    @Value("${kafka.topic.input}")
    private String inputTopic;

    @Value("${kafka.topic.output}")
    private String outputTopic;

    @Value("${message.filter}")
    private String filterValue;

    private final ElasticsearchService elasticsearchService;
    private final Duration inactivityGap = Duration.ofSeconds(30);

    public KafkaStreamsConfig(final ElasticsearchService elasticsearchService) {
        this.elasticsearchService = elasticsearchService;
    }
    @Bean
    public KStream<String, JsonMessage> kStream(StreamsBuilder streamsBuilder) {
        Serde<JsonMessage> jsonMessageSerde = new JsonSerde<>(JsonMessage.class);
        Serde<AggregatedJsonMessage> aggregatedJsonMessageSerde = new JsonSerde<>(AggregatedJsonMessage.class);

        KStream<String, JsonMessage> stream = streamsBuilder
                .stream(inputTopic, Consumed.with(Serdes.String(), jsonMessageSerde))
                .filter((key, jsonMessage) -> {
                    log.info("data: {}, filter: {}, match: {}", jsonMessage, filterValue, jsonMessage.getCommonValue().equalsIgnoreCase(filterValue));
                    return jsonMessage.getCommonValue().equalsIgnoreCase(filterValue);
                })
                .selectKey((key, value) -> value.getId());


        KTable<Windowed<String>, AggregatedJsonMessage> aggregatedTable = stream
                .groupByKey(Grouped.with(Serdes.String(), jsonMessageSerde))
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(inactivityGap))
                .aggregate(
                        AggregatedJsonMessage::new, // Initializer
                        (key, value, aggregate) -> {
                            // Assuming addMessage method correctly updates and returns the aggregate
                            aggregate.addMessage(value);
                            return aggregate;
                        }, // Aggregator
                        (aggKey, aggOne, aggTwo) -> {
                            log.info("Aggregation aggOne:{}, aggTwo: {}", aggOne, aggTwo);
                            return aggOne.merge(aggTwo);
                        }, // Session Merger
                        Materialized.<String, AggregatedJsonMessage, SessionStore<Bytes, byte[]>>as("aggregated-window-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(aggregatedJsonMessageSerde)
                )
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));

        aggregatedTable
                .toStream()
                .peek((key, value) -> log.info("event ingested: {}", value))
                .map((key, value) -> {
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                            .withZone(ZoneId.systemDefault());
                    String windowStartStr = formatter.format(Instant.ofEpochMilli(key.window().start()));
                    String windowEndStr = formatter.format(Instant.ofEpochMilli(key.window().end()));
                    String strKey = String.format("%s@%s-%s", key.key(), windowStartStr, windowEndStr);
                    value.setId(key.key());
                    elasticsearchService.ingestToEs(value);
                    return KeyValue.pair(strKey, value);
                });


        aggregatedTable
                .toStream()
                .filter((key, value) -> value != null && !value.getMessages().isEmpty())
                .foreach((key, value) -> {
            String actualKey = key.key();
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                            .withZone(ZoneId.systemDefault());
                    String start = formatter.format(Instant.ofEpochMilli(key.window().start()));
                    String end = formatter.format(Instant.ofEpochMilli(key.window().end()));
            log.info("Windowed Key: {}, Window Start: {}, Window End: {}, Value: {}", actualKey, start, end, value);
        });

        return stream;
    }




}


