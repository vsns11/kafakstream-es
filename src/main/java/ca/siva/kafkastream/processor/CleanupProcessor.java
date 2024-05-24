//package ca.siva.kafkastream.processor;
//
//import org.apache.kafka.streams.KeyValue;
//import org.apache.kafka.streams.kstream.Windowed;
//
//import org.apache.kafka.streams.processor.PunctuationType;
//import org.apache.kafka.streams.processor.api.Processor;
//import org.apache.kafka.streams.processor.api.ProcessorContext;
//import org.apache.kafka.streams.processor.api.Record;
//import org.apache.kafka.streams.state.KeyValueIterator;
//import org.apache.kafka.streams.state.SessionStore;
//import ca.siva.kafkastream.model.AggregatedData;
//import java.time.Duration;
//import java.time.LocalDateTime;
//import java.time.ZoneId;
//import java.time.format.DateTimeFormatter;
//
//public class CleanupProcessor implements Processor<Windowed<String>, AggregatedData, Void, Void> {
//
//    private ProcessorContext<Void, Void> context;
//    private SessionStore<String, AggregatedData> kvStore;
//
//    @Override
//    public void init(ProcessorContext context) {
//        this.context = context;
//        this.kvStore = context.getStateStore("aggregated-window-store");
//
//        // Schedule the cleanup task to run every 1 minute.
//        context.schedule(Duration.ofMinutes(1), PunctuationType.WALL_CLOCK_TIME, this::cleanupOldEntries);
//    }
//
//    private void cleanupOldEntries(long timestamp) {
//        // Current time for comparison
//        long currentTimeMillis = System.currentTimeMillis();
//        try (KeyValueIterator<Windowed<String>, AggregatedData> iterator = kvStore.findSessions(0, currentTimeMillis)) {
//            while (iterator.hasNext()) {
//                KeyValue<Windowed<String>, AggregatedData> entry = iterator.next();
//                AggregatedData aggregatedData = entry.value;
//                if (isDataOld(aggregatedData, currentTimeMillis)) {
//                    // Remove the session from the store
//                    kvStore.remove(entry.key);
//                }
//            }
//        }
//    }
//
//    private boolean isDataOld(AggregatedData value, long currentTimestamp) {
//        // Assume AggregatedData has getLatestUpdateTimestamp() method returning the timestamp of the last update
//        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
//        long oneDayMillis = Duration.ofDays(1).toMillis();
//        LocalDateTime lastUpdate = LocalDateTime.parse(value.getLatestUpdateTimestamp(), formatter);
//        long lastUpdateMillis = lastUpdate.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
//
//        return (currentTimestamp - lastUpdateMillis) > oneDayMillis;
//    }
//
//
//    @Override
//    public void process(Record record) {
//        //unused
//    }
//
//    @Override
//    public void close() {
//        // Nothing to clean up in this processor
//    }
//}
