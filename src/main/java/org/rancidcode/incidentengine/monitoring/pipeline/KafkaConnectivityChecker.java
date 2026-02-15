package org.rancidcode.incidentengine.monitoring.pipeline;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component
public class KafkaConnectivityChecker {

    private final AdminClient adminClient;

    public KafkaConnectivityChecker(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        this.adminClient = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
    }

    public boolean isKafkaReachable() {
        try {
            adminClient.describeCluster().clusterId().get(2, TimeUnit.SECONDS);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}