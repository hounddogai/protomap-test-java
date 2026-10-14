package com.acme.finance.domains.payroll;



/**
 * An immutable persistence DTO. Snapshot bytes never escape by reference.
 */
public record PayRunRecord(String id, String label, byte[] snapshot) {
    public PayRunRecord {
        snapshot = snapshot.clone();
    }

    @Override
    public byte[] snapshot() {
        return snapshot.clone();
    }
}
