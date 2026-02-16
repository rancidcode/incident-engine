package org.rancidcode.incidentengine.infra.db;

public class DlqTable {
    public static final String TABLE = "dlq";

    public static final String COL_ID = "id";
    public static final String COL_DLQ_TYPE = "dlq_type";
    public static final String COL_SOURCE = "source";
    public static final String COL_EVENT_TIME = "timestamp";
    public static final String COL_RAW_MESSAGE = "raw_message";
    public static final String COL_ERROR_MESSAGE = "error_message";
}
