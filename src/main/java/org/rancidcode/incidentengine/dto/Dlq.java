package org.rancidcode.incidentengine.dto;

import org.rancidcode.incidentengine.common.HasTimestamp;

import java.time.Instant;

public class Dlq implements HasTimestamp {
    private final String dlqType;
    private final String source;
    private final Instant timestamp;
    private final String rawMessage;
    private final String errorMessage;

    public Dlq(String dlqType, String source, Instant timestamp, String rawMessage, String errorMessage) {
        this.dlqType = dlqType;
        this.source = source;
        this.timestamp = timestamp;
        this.rawMessage = rawMessage;
        this.errorMessage = errorMessage;
    }

    public String getDlqType() {
        return dlqType;
    }

    public String getSource() {
        return source;
    }

    @Override
    public Instant getTimestamp() {
        return timestamp;
    }

    public String getRawMessage() {
        return rawMessage;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}
