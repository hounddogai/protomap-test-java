package com.acme.finance.domains.accounting;



/**
 * An immutable persistence DTO. Snapshot bytes never escape by reference.
 */
public record ReconciliationRecord(String id, String label, byte[] snapshot) {
    public ReconciliationRecord {
        snapshot = snapshot.clone();
    }

    @Override
    public byte[] snapshot() {
        return snapshot.clone();
    }
}
