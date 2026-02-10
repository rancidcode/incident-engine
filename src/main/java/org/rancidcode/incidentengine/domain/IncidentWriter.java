package org.rancidcode.incidentengine.domain;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

public class IncidentWriter {
//test
    private static final String INSERT = """
            INSERT INTO incident (
                error_type,
                source,
                status,
                open_time,
                close_time
            )
            VALUES (?, ?, ?, ?, ?) 
            """;

    private void insertIncident(DataSource dataSource, String errorType, String source, String status) {
        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(INSERT)) {
            OffsetDateTime openTime = OffsetDateTime.now(ZoneOffset.UTC);
            ps.setString(1, errorType);
            ps.setString(2, source);
            ps.setString(3, status);
            ps.setObject(4, openTime);
            if (openTime != null) {
                ps.setObject(5, openTime);
            } else {
                ps.setNull(5, java.sql.Types.TIMESTAMP_WITH_TIMEZONE);
            }

            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void addIncident(DataSource dataSource) {
        insertIncident(dataSource, "Test", "TEST", "TEST");
    }
}