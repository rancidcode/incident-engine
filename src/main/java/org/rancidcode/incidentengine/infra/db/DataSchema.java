package org.rancidcode.incidentengine.infra.db;

import org.rancidcode.incidentengine.dto.Incident;
import org.rancidcode.incidentengine.dto.MqttStatus;
import org.rancidcode.incidentengine.dto.Telemetry;
import org.springframework.jdbc.core.RowMapper;
import tools.jackson.databind.ObjectMapper;

public final class DataSchema {
    public static final ObjectMapper MAPPER = new ObjectMapper();

    private DataSchema() {
    }

    public static final RowMapper<Telemetry> telemetryRowMapper = (rs, i) -> new Telemetry(
            rs.getString(TelemetryTable.COL_DEVICE_ID),
            rs.getDouble(TelemetryTable.COL_TEMPERATURE),
            rs.getDouble(TelemetryTable.COL_HEAT_INDEX),
            rs.getDouble(TelemetryTable.COL_HUMIDITY),
            rs.getTimestamp(TelemetryTable.COL_TIMESTAMP).toInstant()
    );

    public static final RowMapper<Incident> incidentRowMapper = (rs, i) -> new Incident(
            rs.getString(IncidentTable.COL_ERROR_TYPE),
            rs.getString(IncidentTable.COL_SOURCE),
            rs.getString(IncidentTable.COL_STATUS),
            rs.getTimestamp(IncidentTable.COL_OPEN_TIME).toInstant(),
            rs.getTimestamp(IncidentTable.COL_CLOSE_TIME).toInstant()
    );

    public static final RowMapper<MqttStatus> mqttStatusRowMapper = (rs, i) -> new MqttStatus(
            rs.getString(MqttStatusTable.COL_EVENT_TYPE),
            rs.getString(MqttStatusTable.COL_STATUS),
            rs.getString(MqttStatusTable.COL_SOURCE),
            rs.getTimestamp(MqttStatusTable.COL_EVENT_TIME).toInstant()
    );

}
