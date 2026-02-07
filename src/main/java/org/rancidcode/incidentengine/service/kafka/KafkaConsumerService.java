package org.rancidcode.incidentengine.service.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaConsumerService {

    @KafkaListener(topics = "${kafka.topic.1m}", groupId = "${kafka.group.raw}")
    public void process1m(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.info("Topic : {}, message : {}", topic, message);
    }

    @KafkaListener(topics = "${kafka.topic.5m}", groupId = "${kafka.group.raw}")
    public void process5m(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.info("Topic : {}, message : {}", topic, message);
    }

    @KafkaListener(topics = "${kafka.topic.dlq}", groupId = "${kafka.group.dlq}")
    public void processDlq(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.info("Topic : {}, message : {}", topic, message);
    }

}