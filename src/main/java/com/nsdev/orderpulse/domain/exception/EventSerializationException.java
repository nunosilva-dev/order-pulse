package com.nsdev.orderpulse.domain.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * Thrown when the system fails to serialize a domain event to JSON.
 * This is a critical infrastructure error that prevents the Outbox pattern from working.
 */
@ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
public class EventSerializationException extends RuntimeException {

    public EventSerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}