package ca.siva.kafkastream.service;


import ca.siva.kafkastream.model.AggregatedJsonMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ElasticsearchService {

    private final ElasticsearchOperations operations;


    public ElasticsearchService(final ElasticsearchOperations operations) {
        this.operations = operations;
    }


    public void ingestToEs(AggregatedJsonMessage data) {
        log.info("Injecting the event to elastic search: {}", data);
        operations.save(data);
    }

}
