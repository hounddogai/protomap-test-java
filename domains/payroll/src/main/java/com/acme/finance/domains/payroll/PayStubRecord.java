package com.acme.finance.domains.payroll;



/**
 * An immutable persistence DTO. Snapshot bytes never escape by reference.
 */
public record PayStubRecord(String id, String label, byte[] snapshot) {
    public PayStubRecord {
        snapshot = snapshot.clone();
    }

    @Override
    public byte[] snapshot() {
        return snapshot.clone();
    }
}
