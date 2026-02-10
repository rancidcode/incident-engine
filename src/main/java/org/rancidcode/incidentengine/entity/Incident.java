package org.rancidcode.incidentengine.entity;

import java.time.Instant;

public class Incident {

    private final String errorType;
    private final String source;
    private final String status;
    private final Instant openTime;
    private final Instant closeTime;

    public Incident(String errorType, String source, String status, Instant openTime, Instant closeTime) {
        this.errorType = errorType;
        this.source = source;
        this.status = status;
        this.openTime = openTime;
        this.closeTime = closeTime;
    }

    public String getErrorType() {
        return errorType;
    }

    public String getSource() {
        return source;
    }

    public String getStatus() {
        return status;
    }

    public Instant getOpenTime() {
        return openTime;
    }

    public Instant getCloseTime() {
        return closeTime;
    }
}