package ca.siva.kafkastream.processor;

import ca.siva.kafkastream.model.AggregatedData;
import ca.siva.kafkastream.service.ElasticsearchService;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;
import java.util.Set;

public class SuppressWindowProcessorSupplier implements ProcessorSupplier<Windowed<String>, AggregatedData, Void, Void> {
    private final ElasticsearchService elasticsearchService;

    public SuppressWindowProcessorSupplier(ElasticsearchService elasticsearchService) {
        this.elasticsearchService = elasticsearchService;
    }

    @Override
    public Processor<Windowed<String>, AggregatedData, Void, Void> get() {
        return new Processor<>() {
            private ProcessorContext<Void, Void> context;
            private Cancellable punctuator;

            @Override
            public void init(ProcessorContext<Void, Void> ctx) {
                this.context = ctx;
                this.punctuator = context.schedule(Duration.ofSeconds(30), PunctuationType.WALL_CLOCK_TIME, new IngestPunctuator(ctx, elasticsearchService));
            }

            @Override
            public void process(Record<Windowed<String>, AggregatedData> record) {
            }

            @Override
            public void close() {
                if (punctuator != null) {
                    punctuator.cancel();
                }
            }
        };
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        return Set.of();
    }
}
