package org.rancidcode.incidentengine.config;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class Scheduler {

    @Scheduled(cron = "${schedulers.incidentChecker.cron}")
    public void scheduleIncidentChecker() {
        System.out.println("schedule");
    }
}