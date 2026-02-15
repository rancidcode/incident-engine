package org.rancidcode.incidentengine.monitoring.pipeline;

import org.rancidcode.incidentengine.adapter.MqttStatusService;
import org.rancidcode.incidentengine.dto.MqttStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/webhooks")
public class MqttConnectivityChecker {
    private final MqttStatusService mqttStatusService;

    public MqttConnectivityChecker(MqttStatusService mqttStatusService) {
        this.mqttStatusService = mqttStatusService;
    }

    @PostMapping("/mqtt-status")
    public void onMqttStatus(@RequestBody MqttStatus mqttStatus) {
        mqttStatusService.handle(mqttStatus);
    }
}