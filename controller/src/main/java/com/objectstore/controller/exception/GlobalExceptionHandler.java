package com.objectstore.controller.exception;

import com.objectstore.common.error.AbstractApiExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Controller-wide translation of exceptions into the shared JSON error contract.
 * Controller-specific handlers are added here as features are implemented.
 */
@RestControllerAdvice
public class GlobalExceptionHandler extends AbstractApiExceptionHandler {
}

