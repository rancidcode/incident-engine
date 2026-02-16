package org.rancidcode.incidentengine.domain;

import lombok.extern.slf4j.Slf4j;
import org.rancidcode.incidentengine.common.HasTimestamp;
import org.rancidcode.incidentengine.dto.Incident;
import org.rancidcode.incidentengine.dto.MqttStatus;
import org.rancidcode.incidentengine.infra.db.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class DataTask {

    public void insertData(JdbcTemplate jdbcTemplate, String tableName, Map<String, Object> map) {
        if (map == null || map.isEmpty()) return;

        String columns = String.join(", ", map.keySet());
        String placeholders = map.keySet().stream().map(k -> "?").collect(Collectors.joining(", "));

        String sql = "INSERT INTO " + tableName + " (" + columns + ") VALUES (" + placeholders + ")";

        jdbcTemplate.update(sql, map.values().toArray());

        log.info("Insert data into " + tableName);
    }

    private <T extends HasTimestamp> long getLastDataEpochSeconds(JdbcTemplate jdbcTemplate, String table, String timestampCol, RowMapper<T> rowMapper) {
        List<T> list = jdbcTemplate.query("SELECT * FROM " + table + " ORDER BY " + timestampCol + " DESC LIMIT 1", rowMapper);
        if (!list.isEmpty()) {
            return list.get(0).getTimestamp().getEpochSecond();
        }
        return 0;
    }

    public boolean isLastIncidentOpen(JdbcTemplate jdbcTemplate) {
        List<Incident> list = jdbcTemplate.query("SELECT * FROM " + IncidentTable.TABLE + " ORDER BY " + IncidentTable.COL_ID + " DESC LIMIT 1", DataSchema.incidentRowMapper);
        if (!list.isEmpty()) {
            if (list.get(0).getStatus().equalsIgnoreCase("CLOSED")) return true;
        }
        return false;
    }

    public boolean isMqttConnnected(JdbcTemplate jdbcTemplate) {
        List<MqttStatus> list = jdbcTemplate.query("SELECT * FROM " + MqttStatusTable.TABLE + " ORDER BY " + MqttStatusTable.COL_ID + " DESC LIMIT 1", DataSchema.mqttStatusRowMapper);
        if (!list.isEmpty()) {
            if (list.get(0).getStatus().equalsIgnoreCase("CONNECTED")) return true;
        }
        return false;
    }

    public boolean dataStopped(JdbcTemplate jdbcTemplate) {
        long lastDataTime = getLastDataEpochSeconds(jdbcTemplate, TelemetryTable.TABLE, TelemetryTable.COL_TIMESTAMP, DataSchema.telemetryRowMapper);
        long currentTime = Instant.now().getEpochSecond();
        log.info("lastDataTime : {}, currentTime : {}", lastDataTime, currentTime);

        if (currentTime - lastDataTime > 30) {
            log.info("{}", currentTime - lastDataTime);
            return true;
        }
        return false;
    }

    public boolean dataDlq(JdbcTemplate jdbcTemplate) {
        long lastDataTimeDlq = getLastDataEpochSeconds(jdbcTemplate, DlqTable.TABLE, DlqTable.COL_EVENT_TIME, DataSchema.dlqRowMapper);
        long lastDataTimeRaw = getLastDataEpochSeconds(jdbcTemplate, TelemetryTable.TABLE, TelemetryTable.COL_TIMESTAMP, DataSchema.telemetryRowMapper);

        if (lastDataTimeDlq > lastDataTimeRaw) return true;

        return false;
    }
}