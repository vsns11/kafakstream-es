package ca.siva.kafkastream.service;


import ca.siva.kafkastream.model.AggregatedData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@Service
@Slf4j
public class ElasticsearchService {

    private final ElasticsearchOperations operations;


    public ElasticsearchService(final ElasticsearchOperations operations) {
        this.operations = operations;
    }


    public IndexCoordinates indexCoordinates() {
        String currentDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        String indexName = "aggregated-info"+currentDate;
        log.info("Injecting the event to elasticsearch in index: {}", indexName);
        return IndexCoordinates.of(indexName);
    }
    public void ingestToEs(AggregatedData data) {

        operations.save(data, indexCoordinates());
    }

}
