package ca.siva.kafkastream.model;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.elasticsearch.annotations.Document;

import java.util.ArrayList;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(indexName = "aggregated-info")
public class AggregatedJsonMessage {
    private String id;
    private List<JsonMessage> messages = new ArrayList<>();

    public AggregatedJsonMessage merge(AggregatedJsonMessage aggregatedJsonMessage) {
        this.messages.addAll(aggregatedJsonMessage.getMessages());
        return this;
    }

    public AggregatedJsonMessage addMessage(JsonMessage jsonMessage) {
        this.messages.add(jsonMessage);
        return this;
    }

}
