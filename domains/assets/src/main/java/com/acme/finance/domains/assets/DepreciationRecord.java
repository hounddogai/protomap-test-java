package com.acme.finance.domains.assets;



/**
 * An immutable persistence DTO. Snapshot bytes never escape by reference.
 */
public record DepreciationRecord(String id, String label, byte[] snapshot) {
    public DepreciationRecord {
        snapshot = snapshot.clone();
    }

    @Override
    public byte[] snapshot() {
        return snapshot.clone();
    }
}
