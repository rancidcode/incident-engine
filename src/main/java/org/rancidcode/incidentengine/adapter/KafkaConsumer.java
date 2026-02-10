package org.rancidcode.incidentengine.adapter;

import lombok.extern.slf4j.Slf4j;
import org.rancidcode.incidentengine.domain.DataTask;
import org.rancidcode.incidentengine.entity.Telemetry;
import org.rancidcode.incidentengine.infra.db.TelemetryTable;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.Map;

@Service
@Slf4j
public class KafkaConsumer {

    private final JdbcTemplate jdbcTemplate;
    private final DataTask dataTask = new DataTask();
    private final ObjectMapper MAPPER = new ObjectMapper();

    public KafkaConsumer(@Qualifier("telemetryDataSource") DataSource telemetryDataSource) {
        this.jdbcTemplate = new JdbcTemplate(telemetryDataSource);
    }

    @KafkaListener(topics = "${kafka.topic.1m}", groupId = "${kafka.group.avg}")
    public void processAvg(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.info("Topic : {}, message : {}", topic, message);
    }

    @KafkaListener(topics = "${kafka.topic.dlq}", groupId = "${kafka.group.dlq}")
    public void processDlq(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.info("Topic : {}, message : {}", topic, message);
    }

    @KafkaListener(topics = "${kafka.topic.raw}", groupId = "${kafka.group.raw}")
    public void processRaw(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.info("Topic : {}, message : {}", topic, message);

        dataTask.insertData(jdbcTemplate, extractTelemetry(message));
    }

    private Map<String, Object> extractTelemetry(String rawMessage) {
        //id | device_id | temperature | heat_index | humidity | event_time
        ObjectNode objectNode = null;
        Map<String, Object> values = null;

        try {
            objectNode = (ObjectNode) MAPPER.readTree(rawMessage);
            Telemetry telemetry = MAPPER.treeToValue(objectNode, Telemetry.class);

            values = new LinkedHashMap<>();
            values.put(TelemetryTable.COL_DEVICE_ID, telemetry.getDeviceId());
            values.put(TelemetryTable.COL_TEMPERATURE, telemetry.getTemperature());
            values.put(TelemetryTable.COL_HEAT_INDEX, telemetry.getHeatIndex());
            values.put(TelemetryTable.COL_HUMIDITY, telemetry.getHumidity());
            values.put(TelemetryTable.COL_TIMESTAMP, Timestamp.from(telemetry.getTimestamp()));

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return values;
    }
}