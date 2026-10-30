package com.objectstore.common.dto;

/**
 * Lifecycle status of an object in the controller metadata store (plan.md D14).
 */
public enum ObjectStatus {
    UPLOADING,
    ACTIVE,
    DELETING,
    FAILED
}

