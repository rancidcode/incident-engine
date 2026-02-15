package org.rancidcode.incidentengine.infra.db;

public class MqttStatusTable {
    public static final String TABLE = "mqtt_status";

    public static final String COL_ID = "id";
    public static final String COL_EVENT_TYPE = "event_type";
    public static final String COL_STATUS = "status";
    public static final String COL_SOURCE = "source";
    public static final String COL_EVENT_TIME = "event_time";
}
