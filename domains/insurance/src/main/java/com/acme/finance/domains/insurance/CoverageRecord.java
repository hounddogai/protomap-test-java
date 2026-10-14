package com.acme.finance.domains.insurance;



/**
 * An immutable persistence DTO. Snapshot bytes never escape by reference.
 */
public record CoverageRecord(String id, String label, byte[] snapshot) {
    public CoverageRecord {
        snapshot = snapshot.clone();
    }

    @Override
    public byte[] snapshot() {
        return snapshot.clone();
    }
}
