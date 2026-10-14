package com.acme.finance.domains.insurance;



/**
 * An immutable persistence DTO. Snapshot bytes never escape by reference.
 */
public record PolicyRecord(String id, String label, byte[] snapshot) {
    public PolicyRecord {
        snapshot = snapshot.clone();
    }

    @Override
    public byte[] snapshot() {
        return snapshot.clone();
    }
}
