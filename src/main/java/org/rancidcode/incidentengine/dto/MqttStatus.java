package org.rancidcode.incidentengine.dto;

import java.time.Instant;

public class MqttStatus {
    private String eventType;
    private String status;
    private String source;
    private Instant eventTime;

    public MqttStatus(String eventType, String status, String source, Instant eventTime) {
        this.eventType = eventType;
        this.status = status;
        this.eventTime = eventTime;
        this.source = source;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Instant getEventTime() {
        return eventTime;
    }

    public void setEventTime(Instant eventTime) {
        this.eventTime = eventTime;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }
}
