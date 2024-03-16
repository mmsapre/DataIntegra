package com.integration.em.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DurationFormatUtils;

import java.time.Duration;
import java.time.LocalDateTime;

@Slf4j
public class ProgressReporter {

    private int done = 0;
    private LocalDateTime lastTime;
    private int total = 0;
    private LocalDateTime start;
    private String message;

    public ProgressReporter(int totalElements, String message) {
        total = totalElements;
        start = LocalDateTime.now();
        lastTime = start;
        this.message = message;
    }

    public ProgressReporter(int totalElements, String message, int processedElements) {
        total = totalElements;
        start = LocalDateTime.now();
        lastTime = start;
        this.message = message;
        this.done = processedElements;
    }

    public void incrementProgress() {
        done++;
    }

    public void report() {
        // report status every second
        LocalDateTime now = LocalDateTime.now();
        long durationSoFar = Duration.between(start, now).toMillis();
        if ((Duration.between(lastTime, now).toMillis()) > 1000) {
            if(total>0) {
                log.info(String.format(
                        "%s: %,d / %,d elements completed (%.2f%%) after %s",
                        message, done, total,
                        (double) done / (double) total * 100,
                        DurationFormatUtils.formatDurationHMS(durationSoFar)));
            } else {
                log.info(String.format(
                        "%s: %,d elements completed after %s",
                        message, done,
                        DurationFormatUtils.formatDurationHMS(durationSoFar)));
            }
            lastTime = now;
        }
    }

    public int getProcessedElements() {
        return done;
    }

    public void setProcessedElements(int done) {
        this.done = done;
    }

    public LocalDateTime getLastTime() {
        return lastTime;
    }

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public LocalDateTime getStart() {
        return start;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
