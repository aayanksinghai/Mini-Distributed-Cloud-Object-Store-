package com.objectstore.common.dto;

import java.util.List;

/**
 * Paginated list of stored objects returned by {@code GET /api/v1/objects}.
 */
public record ObjectListPage(
        List<ObjectInfo> items,
        int page,
        int size,
        long totalItems,
        int totalPages) {

    public ObjectListPage {
        items = items == null ? List.of() : List.copyOf(items);
    }
}

