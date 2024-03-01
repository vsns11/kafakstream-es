package ca.siva.kafkastream.model;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AggregatedData {
    private String id;

    private List<Map<String, Object>> relatedEvents = new ArrayList<>();

    public AggregatedData merge(AggregatedData aggregatedJsonMessage) {
        this.relatedEvents.addAll(aggregatedJsonMessage.getRelatedEvents());
        return this;
    }

    public AggregatedData add(Map<String, Object> jsonMessage) {
        this.relatedEvents.add(jsonMessage);
        return this;
    }

}
