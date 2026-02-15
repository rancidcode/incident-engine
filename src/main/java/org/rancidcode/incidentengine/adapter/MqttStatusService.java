package org.rancidcode.incidentengine.adapter;

import org.rancidcode.incidentengine.domain.DataTask;
import org.rancidcode.incidentengine.dto.MqttStatus;
import org.rancidcode.incidentengine.infra.db.MqttStatusTable;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.Map;

@Service
public class MqttStatusService {

    private final JdbcTemplate jdbcIncident;
    private final DataTask dataTask = new DataTask();

    public MqttStatusService(@Qualifier("incidentDataSource") DataSource incidentDataSource) {
        this.jdbcIncident = new JdbcTemplate(incidentDataSource);
    }

    public void handle(MqttStatus mqttStatus) {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put(MqttStatusTable.COL_EVENT_TYPE, mqttStatus.getEventType());
        values.put(MqttStatusTable.COL_STATUS, mqttStatus.getStatus());
        values.put(MqttStatusTable.COL_SOURCE, mqttStatus.getSource());
        values.put(MqttStatusTable.COL_EVENT_TIME, Timestamp.from(mqttStatus.getEventTime()));

        dataTask.insertData(jdbcIncident, MqttStatusTable.TABLE, values);
    }
}