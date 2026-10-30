package com.objectstore.worker.exception;

import com.objectstore.common.error.AbstractApiExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Worker-wide translation of exceptions into the shared JSON error contract.
 * Worker-specific handlers are added here as features are implemented.
 */
@RestControllerAdvice
public class GlobalExceptionHandler extends AbstractApiExceptionHandler {
}

