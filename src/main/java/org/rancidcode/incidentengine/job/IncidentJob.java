package org.rancidcode.incidentengine.job;

import lombok.extern.slf4j.Slf4j;
import org.rancidcode.incidentengine.domain.DataTask;
import org.rancidcode.incidentengine.domain.enums.IncidentType;
import org.rancidcode.incidentengine.infra.db.IncidentTable;
import org.rancidcode.incidentengine.monitoring.pipeline.KafkaConnectivityChecker;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import tools.jackson.databind.node.ObjectNode;

import javax.sql.DataSource;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
@Component
public class IncidentJob {

    private final JdbcTemplate jdbcTelemetry;
    private final JdbcTemplate jdbcIncident;
    private final KafkaConnectivityChecker kafkaConnectivityChecker;
    private final DataTask dataTask = new DataTask();

    public IncidentJob(@Qualifier("telemetryDataSource") DataSource telemetryDataSource, @Qualifier("incidentDataSource") DataSource incidentDataSource, KafkaConnectivityChecker kafkaConnectivityChecker) {
        this.jdbcIncident = new JdbcTemplate(incidentDataSource);
        this.jdbcTelemetry = new JdbcTemplate(telemetryDataSource);
        this.kafkaConnectivityChecker = kafkaConnectivityChecker;
    }

    @Scheduled(cron = "${schedulers.incidentChecker.cron}")
    public void scheduleIncidentChecker() {
        System.out.println("schedule");

        if (dataTask.dataStopped(jdbcIncident) && !dataTask.isLastIncidentOpen(jdbcIncident)) {
            String errorType = IncidentType.NK.name();

            if (!kafkaConnectivityChecker.isKafkaReachable()) errorType = IncidentType.KAFKA_DISCONNECTED.name();
            else if (!dataTask.isMqttConnnected(jdbcTelemetry)) errorType = IncidentType.MQTT_DISCONNECTED.name();
            else if (dataTask.dataDlq(jdbcTelemetry)) errorType = IncidentType.INVALID_DATA.name();
            else errorType = IncidentType.DEVICE_OFFLINE.name();

            insertIncident(errorType, "", "OPEN", Instant.now(), null);
        }
    }

    private void insertIncident(String errorType, String source, String status, Instant open, Instant closed) {
        //id | error_type | source | status | open_time | close_time
        ObjectNode objectNode = null;
        Map<String, Object> values = null;

        values = new LinkedHashMap<>();
        values.put(IncidentTable.COL_ERROR_TYPE, "");
        values.put(IncidentTable.COL_SOURCE, "");
        values.put(IncidentTable.COL_STATUS, "");
        values.put(IncidentTable.COL_OPEN_TIME, "");
        values.put(IncidentTable.COL_CLOSE_TIME, "");
    }
}