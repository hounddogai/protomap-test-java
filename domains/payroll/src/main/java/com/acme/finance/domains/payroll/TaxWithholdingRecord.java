package com.acme.finance.domains.payroll;



/**
 * An immutable persistence DTO. Snapshot bytes never escape by reference.
 */
public record TaxWithholdingRecord(String id, String label, byte[] snapshot) {
    public TaxWithholdingRecord {
        snapshot = snapshot.clone();
    }

    @Override
    public byte[] snapshot() {
        return snapshot.clone();
    }
}
