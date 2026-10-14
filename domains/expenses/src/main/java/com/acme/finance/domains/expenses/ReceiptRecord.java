package com.acme.finance.domains.expenses;



/**
 * An immutable persistence DTO. Snapshot bytes never escape by reference.
 */
public record ReceiptRecord(String id, String label, byte[] snapshot) {
    public ReceiptRecord {
        snapshot = snapshot.clone();
    }

    @Override
    public byte[] snapshot() {
        return snapshot.clone();
    }
}
