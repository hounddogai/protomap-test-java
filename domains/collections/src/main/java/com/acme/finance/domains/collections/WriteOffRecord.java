package com.acme.finance.domains.collections;



/**
 * An immutable persistence DTO. Snapshot bytes never escape by reference.
 */
public record WriteOffRecord(String id, String label, byte[] snapshot) {
    public WriteOffRecord {
        snapshot = snapshot.clone();
    }

    @Override
    public byte[] snapshot() {
        return snapshot.clone();
    }
}
