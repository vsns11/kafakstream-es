package ca.siva.kafkastream.processor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.streams.messaging.MessagingFunction;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

@Slf4j
public class LoggingMessagingFunction implements MessagingFunction {
    @Override
    public Message<?> exchange(Message<?> message) {
        Long offset = (Long) message.getHeaders().get(KafkaHeaders.OFFSET);
        String key = (String) message.getHeaders().get(KafkaHeaders.RECEIVED_KEY);
        log.info("Processing message with key:{} at offset:{}",  key, offset);
        return message;
    }
}
