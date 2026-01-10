package com.nsdev.orderpulse.infra.outbox.scheduler;

import com.nsdev.orderpulse.infra.outbox.model.OutboxEvent;
import com.nsdev.orderpulse.infra.outbox.model.OutboxStatus;
import com.nsdev.orderpulse.infra.outbox.repository.OutboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;

/**
 * Scheduler responsible for processing the Transactional Outbox pattern.
 * <p>
 * Ensures eventual delivery of messages to Kafka, even in cases of temporary
 * network failures or broker unavailability.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxScheduler {

    private final OutboxRepository outboxRepository;
    private final StreamBridge streamBridge;

    @Value("${orderpulse.outbox.max-retries:5}")
    private int maxRetries;

    /**
     * The Main "Poller".
     * <p>
     * Runs frequently to process new events created by the application.
     * Reads only events with {@link OutboxStatus#PENDING} status.
     * <p>
     * Configuration: {@code orderpulse.outbox.polling-interval-ms} (Default: 5s)
     */
    @Scheduled(fixedDelayString = "${orderpulse.outbox.polling-interval-ms:5000}")
    public void processOutboxEvents() {
        List<OutboxEvent> pendingEvents = outboxRepository.findByStatus(OutboxStatus.PENDING);
        if (pendingEvents.isEmpty()) {
            return;
        }
        log.debug("Processing {} pending events...", pendingEvents.size());
        for (OutboxEvent event : pendingEvents) {
            try {
                processSingleEvent(event);
            } catch (Exception e) {
                handleFailure(event, e);
            }
        }
    }

    /**
     * The "Hospital Queue" (Failure Recovery).
     * <p>
     * Runs less frequently to attempt recovery of events that failed repeatedly
     * (Status {@link OutboxStatus#FAILED}).
     * If it fails at this stage, the event is moved to {@link OutboxStatus#MANUAL_INTERVENTION}.
     * <p>
     * Configuration: {@code orderpulse.outbox.hospital-interval-ms} (Default: 30min)
     */
    @Scheduled(fixedRateString = "${orderpulse.outbox.hospital-interval-ms:1800000}")
    public void processFailedEvents() {
        List<OutboxEvent> failedEvents = outboxRepository.findByStatus(OutboxStatus.FAILED);

        if (failedEvents.isEmpty()) {
            return;
        }

        log.info("🚑 Hospital Queue: Found {} failed events. Attempting recovery...", failedEvents.size());

        for (OutboxEvent event : failedEvents) {
            try {
                log.info("🚑 Re-attempting EventID: {}", event.getId());
                processSingleEvent(event);
                log.info("✅ Event recovered from FAILED status! ID: {}", event.getId());

            } catch (Exception e) {
                log.error("💀 Recovery failed for EventID: {}. Moving to MANUAL_INTERVENTION.", event.getId());

                event.setStatus(OutboxStatus.MANUAL_INTERVENTION);
                event.setLastError("Hospital recovery failed: " + e.getMessage());
                event.setLastAttempt(Instant.now());

                outboxRepository.save(event);
            }
        }
    }

    /**
     * Attempts to send a single event to Kafka and marks it as COMPLETED.
     *
     * @param event The event to be processed
     * @throws RuntimeException if the sending fails (triggering retry logic)
     */
    private void processSingleEvent(OutboxEvent event) {
        log.debug("Attempting to send EventID: {}", event.getId());

        Message<String> message = MessageBuilder
                .withPayload(event.getPayload())
                .setHeader("eventId", event.getId())
                .setHeader("aggregateId", event.getAggregateId())
                .setHeader("eventType", event.getEventType())
                .build();

        // Sending is synchronous (configured in application.yml). Throws exception on failure.
        streamBridge.send("orderCreated-out-0", message);

        event.setStatus(OutboxStatus.COMPLETED);
        event.setLastAttempt(Instant.now());
        outboxRepository.save(event);

        log.info("✅ Event processed. ID: {}", event.getId());
    }

    /**
     * Manages retry logic and Dead Letter Queue (DLQ).
     *
     * @param event The event that failed
     * @param e     The exception that caused the failure
     */
    private void handleFailure(OutboxEvent event, Exception e) {
        int currentRetries = event.getRetryCount() + 1;
        event.setRetryCount(currentRetries);
        event.setLastAttempt(Instant.now());
        event.setLastError(e.getMessage());

        log.error("❌ Failed to process event ID: {}. Attempt {}/{}", event.getId(), currentRetries, maxRetries);

        if (currentRetries >= maxRetries) {
            log.error("💀 Max retries reached for EventID: {}. Moving to FAILED status.", event.getId());
            event.setStatus(OutboxStatus.FAILED);
        }

        outboxRepository.save(event);
    }
}