package com.acme.finance.domains.collections;



/**
 * An immutable persistence DTO. Snapshot bytes never escape by reference.
 */
public record CollectionCaseRecord(String id, String label, byte[] snapshot) {
    public CollectionCaseRecord {
        snapshot = snapshot.clone();
    }

    @Override
    public byte[] snapshot() {
        return snapshot.clone();
    }
}
