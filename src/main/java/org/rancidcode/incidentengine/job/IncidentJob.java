package org.rancidcode.incidentengine.job;

import lombok.extern.slf4j.Slf4j;
import org.rancidcode.incidentengine.domain.IncidentWriter;
import org.rancidcode.incidentengine.entity.Incident;
import org.rancidcode.incidentengine.entity.Telemetry;
import org.rancidcode.incidentengine.infra.db.DataSchema;
import org.rancidcode.incidentengine.infra.db.TelemetryTable;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

import java.time.Instant;
import java.util.List;

@Slf4j
@Component
public class IncidentJob {

    private final JdbcTemplate jdbcTelemetry;
    private final JdbcTemplate jdbcIncident;

    public IncidentJob(@Qualifier("telemetryDataSource") DataSource telemetryDataSource, @Qualifier("incidentDataSource") DataSource incidentDataSource) {
        this.jdbcIncident = new JdbcTemplate(incidentDataSource);
        this.jdbcTelemetry = new JdbcTemplate(telemetryDataSource);
    }

    @Scheduled(cron = "${schedulers.incidentChecker.cron}")
    public void scheduleIncidentChecker() {
        System.out.println("schedule");
        if (dataStopped() && !isLastIncidentOpen()) System.out.println("run");
    }

    private boolean dataStopped() {
        List<Telemetry> list = jdbcTelemetry.query("SELECT * FROM telemetry ORDER BY timestamp DESC", DataSchema.telemetryRowMapper);

        if (!list.isEmpty()) {
            long currentTime = Instant.now().getEpochSecond();
            long lastDataTime = list.get(0).getTimestamp().getEpochSecond();
            log.info("lastDataTime : {}, currentTime : {}", lastDataTime, currentTime);

            if (currentTime - lastDataTime > 30) {
                log.info("{}", currentTime - lastDataTime);
                return true;
            }
        }
        return false;
    }

    private boolean isLastIncidentOpen() {
        List<Incident> list = jdbcIncident.query("SELECT * FROM incident ORDER BY id DESC", DataSchema.incidentRowMapper);
        if (!list.isEmpty()) {
            if (list.get(0).getStatus().equalsIgnoreCase("CLOSED")) return true;
        }
        return false;
    }
}