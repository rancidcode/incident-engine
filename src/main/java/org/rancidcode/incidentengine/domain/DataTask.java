package org.rancidcode.incidentengine.domain;

import org.rancidcode.incidentengine.infra.db.TelemetryTable;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Map;
import java.util.stream.Collectors;

public class DataTask {

    public void insertData(JdbcTemplate jdbcTemplate, Map<String, Object> map) {
        if (map == null || map.isEmpty()) return;

        String columns = String.join(", ", map.keySet());
        String placeholders = map.keySet().stream().map(k -> "?").collect(Collectors.joining(", "));

        String sql = "INSERT INTO " + TelemetryTable.TABLE + " (" + columns + ") VALUES (" + placeholders + ")";

        jdbcTemplate.update(sql, map.values().toArray());
    }
}