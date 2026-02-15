package org.rancidcode.incidentengine.job;

import lombok.extern.slf4j.Slf4j;
import org.rancidcode.incidentengine.domain.enums.IncidentType;
import org.rancidcode.incidentengine.dto.Incident;
import org.rancidcode.incidentengine.dto.MqttStatus;
import org.rancidcode.incidentengine.dto.Telemetry;
import org.rancidcode.incidentengine.infra.db.DataSchema;
import org.rancidcode.incidentengine.infra.db.IncidentTable;
import org.rancidcode.incidentengine.infra.db.MqttStatusTable;
import org.rancidcode.incidentengine.infra.db.TelemetryTable;
import org.rancidcode.incidentengine.monitoring.pipeline.KafkaConnectivityChecker;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import tools.jackson.databind.node.ObjectNode;

import javax.sql.DataSource;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class IncidentJob {

    private final JdbcTemplate jdbcTelemetry;
    private final JdbcTemplate jdbcIncident;
    private final KafkaConnectivityChecker kafkaConnectivityChecker;

    public IncidentJob(@Qualifier("telemetryDataSource") DataSource telemetryDataSource, @Qualifier("incidentDataSource") DataSource incidentDataSource, KafkaConnectivityChecker kafkaConnectivityChecker) {
        this.jdbcIncident = new JdbcTemplate(incidentDataSource);
        this.jdbcTelemetry = new JdbcTemplate(telemetryDataSource);
        this.kafkaConnectivityChecker = kafkaConnectivityChecker;
    }

    @Scheduled(cron = "${schedulers.incidentChecker.cron}")
    public void scheduleIncidentChecker() {
        System.out.println("schedule");
        if (dataStopped() && !isLastIncidentOpen()) {
            String errorType = IncidentType.NK.name();

            if (!kafkaConnectivityChecker.isKafkaReachable()) errorType = IncidentType.KAFKA_DISCONNECTED.name();
            else if (!isMqttConnnected()) errorType = IncidentType.MQTT_DISCONNECTED.name();

            //insertIncident(errorType, "", "OPEN", Instant.now(), null);
        }
    }

    private boolean dataStopped() {
        List<Telemetry> list = jdbcTelemetry.query("SELECT * FROM " + TelemetryTable.TABLE + " ORDER BY " + TelemetryTable.COL_TIMESTAMP + " DESC", DataSchema.telemetryRowMapper);
        if (!list.isEmpty()) {
            long currentTime = Instant.now().getEpochSecond();
            long lastDataTime = list.get(0).getTimestamp().getEpochSecond();
            log.info("lastDataTime : {}, currentTime : {}", lastDataTime, currentTime);

            if (currentTime - lastDataTime > 30) {
                log.info("{}", currentTime - lastDataTime);
                return true;
            }
        }
        return false;
    }

    private boolean isLastIncidentOpen() {
        List<Incident> list = jdbcIncident.query("SELECT * FROM " + IncidentTable.TABLE + " ORDER BY " + IncidentTable.COL_ID + " DESC", DataSchema.incidentRowMapper);
        if (!list.isEmpty()) {
            if (list.get(0).getStatus().equalsIgnoreCase("CLOSED")) return true;
        }
        return false;
    }

    private boolean isMqttConnnected() {
        List<MqttStatus> list = jdbcIncident.query("SELECT * FROM " + MqttStatusTable.TABLE + " ORDER BY " + MqttStatusTable.COL_ID + " DESC", DataSchema.mqttStatusRowMapper);
        if (!list.isEmpty()) {
            if (list.get(0).getStatus().equalsIgnoreCase("CONNECTED")) return true;
        }
        return false;
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