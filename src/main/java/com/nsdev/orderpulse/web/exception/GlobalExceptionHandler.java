package com.nsdev.orderpulse.web.exception;

import com.nsdev.orderpulse.domain.exception.EventSerializationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.net.URI;
import java.time.Instant;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(EventSerializationException.class)
    public ProblemDetail handleSerializationException(EventSerializationException ex) {
        log.error("Caught serialization error: {}", ex.getMessage());
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
        problem.setTitle("Event Serialization Error");
        // Uniform Resource Name
        problem.setType(URI.create("urn:orderpulse:error:serialization"));
        problem.setProperty("timestamp", Instant.now());
        return problem;
    }
}