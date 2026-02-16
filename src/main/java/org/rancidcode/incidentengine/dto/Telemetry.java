package org.rancidcode.incidentengine.dto;

import org.rancidcode.incidentengine.common.HasTimestamp;

import java.time.Instant;

public class Telemetry implements HasTimestamp {

    private final String deviceId;
    private final double temperature;
    private final double heatIndex;
    private final double humidity;
    private final Instant timestamp;

    public Telemetry(String deviceId, double temperature, double heatIndex, double humidity, Instant timestamp) {
        this.deviceId = deviceId;
        this.temperature = temperature;
        this.heatIndex = heatIndex;
        this.humidity = humidity;
        this.timestamp = timestamp;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public double getTemperature() {
        return temperature;
    }

    public double getHeatIndex() {
        return heatIndex;
    }

    public double getHumidity() {
        return humidity;
    }

    @Override
    public Instant getTimestamp() {
        return timestamp;
    }
}
