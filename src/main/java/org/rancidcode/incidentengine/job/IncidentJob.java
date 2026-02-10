package org.rancidcode.incidentengine.job;

import org.rancidcode.incidentengine.domain.IncidentWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@Component
public class IncidentJob {

    private final DataSource incidentDataSource;
    private final DataSource telemetryDataSource;
    private final IncidentWriter incidentWriter = new IncidentWriter();

    public IncidentJob(@Qualifier("telemetryDataSource") DataSource telemetryDataSource, @Qualifier("incidentDataSource") DataSource incidentDataSource) {
        this.telemetryDataSource = telemetryDataSource;
        this.incidentDataSource = incidentDataSource;
    }

    @Scheduled(cron = "${schedulers.incidentChecker.cron}")
    public void scheduleIncidentChecker() {
        System.out.println("schedule");
        //incidentWriter.addIncident(dataSource);
        checkIncident();
    }

    public void checkIncident() {
        String sql = "SELECT * FROM telemetry";
        ResultSet result = null;

        try (Connection conn = telemetryDataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            result = ps.executeQuery();

            while (result.next()) {
                String lastDateTime = result.getString("timestamp");
                System.out.println(lastDateTime);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}