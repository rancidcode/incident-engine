package org.rancidcode.incidentengine.infra.db;

public class IncidentTable {
    //id | error_type | source | status | open_time | close_time
    public static final String TABLE = "incident";

    public static final String COL_ID = "id";
    public static final String COL_ERROR_TYPE = "error_type";
    public static final String COL_SOURCE = "source";
    public static final String COL_STATUS = "status";
    public static final String COL_OPEN_TIME = "open_time";
    public static final String COL_CLOSE_TIME = "close_time";
}
