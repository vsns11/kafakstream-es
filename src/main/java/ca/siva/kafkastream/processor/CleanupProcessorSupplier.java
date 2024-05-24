//package ca.siva.kafkastream.processor;
//
//import ca.siva.kafkastream.model.AggregatedData;
//import org.apache.kafka.streams.kstream.Windowed;
//import org.apache.kafka.streams.processor.api.Processor;
//import org.apache.kafka.streams.processor.api.ProcessorSupplier;
//
//public class CleanupProcessorSupplier implements ProcessorSupplier<Windowed<String>, AggregatedData, Void, Void> {
//    @Override
//    public Processor<Windowed<String>, AggregatedData, Void, Void> get() {
//        // Always return a new instance of CleanupProcessor
//        return new CleanupProcessor();
//    }
//}
