package com.acme.finance.domains.billing;



/**
 * An immutable persistence DTO. Snapshot bytes never escape by reference.
 */
public record BillingAccountRecord(String id, String label, byte[] snapshot) {
    public BillingAccountRecord {
        snapshot = snapshot.clone();
    }

    @Override
    public byte[] snapshot() {
        return snapshot.clone();
    }
}
