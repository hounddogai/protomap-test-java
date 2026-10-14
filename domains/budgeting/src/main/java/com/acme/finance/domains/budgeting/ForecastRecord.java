package com.acme.finance.domains.budgeting;



/**
 * An immutable persistence DTO. Snapshot bytes never escape by reference.
 */
public record ForecastRecord(String id, String label, byte[] snapshot) {
    public ForecastRecord {
        snapshot = snapshot.clone();
    }

    @Override
    public byte[] snapshot() {
        return snapshot.clone();
    }
}
