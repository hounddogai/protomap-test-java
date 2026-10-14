package com.acme.finance.domains.accounting;



/**
 * An immutable persistence DTO. Snapshot bytes never escape by reference.
 */
public record AllocationRecord(String id, String label, byte[] snapshot) {
    public AllocationRecord {
        snapshot = snapshot.clone();
    }

    @Override
    public byte[] snapshot() {
        return snapshot.clone();
    }
}
