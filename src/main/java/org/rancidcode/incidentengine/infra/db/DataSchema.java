package org.rancidcode.incidentengine.infra.db;

import org.rancidcode.incidentengine.entity.Incident;
import org.rancidcode.incidentengine.entity.Telemetry;
import org.springframework.jdbc.core.RowMapper;

public final class DataSchema
{
    private DataSchema(){}

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
}
