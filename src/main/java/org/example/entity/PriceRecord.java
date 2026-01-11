package org.example.entity;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable representation of a price record.
 */
public final class PriceRecord {

    private final String id;
    private final Instant asOf;
    private final Map<String, Object> payload;

    public PriceRecord(String id, Instant asOf, Map<String, Object> payload) {
        this.id = Objects.requireNonNull(id, "id");
        this.asOf = Objects.requireNonNull(asOf, "asOf");
        this.payload = payload; // payload can be null or mutable
    }

    public String getId() {
        return id;
    }

    public Instant getAsOf() {
        return asOf;
    }

    public Map<String, Object> getPayload() {
        return payload;
    }
}
