package ca.siva.kafkastream.processor;

import ca.siva.kafkastream.model.AggregatedData;
import ca.siva.kafkastream.service.ElasticsearchService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.state.*;

@Slf4j
public class IngestPunctuator implements Punctuator {

    private final ProcessorContext<Void, Void> context;
    private final ElasticsearchService elasticsearchService;

    public IngestPunctuator(ProcessorContext<Void, Void> context, ElasticsearchService elasticsearchService) {
        this.context = context;
        this.elasticsearchService = elasticsearchService;
    }

    @Override
    public void punctuate(long timestamp) {
        log.info("Punctuate called at: {}", timestamp);
        SessionStore<String, AggregatedData> sessionStore = context.getStateStore("aggregated-window-store");

        try (KeyValueIterator<Windowed<String>, AggregatedData> iterator = sessionStore.fetch(null, null)) {
            while (iterator.hasNext()) {
                KeyValue<Windowed<String>, AggregatedData> keyValue = iterator.next();
                log.info("Ingesting to Elasticsearch: key = {}, value = {}", keyValue.key.key(), keyValue.value);
                elasticsearchService.ingestToEs(keyValue.value);
                sessionStore.remove(keyValue.key);
            }
        } catch (Exception e) {
            log.error("Error during punctuate: {}", e.getMessage(), e);
        }
    }
}