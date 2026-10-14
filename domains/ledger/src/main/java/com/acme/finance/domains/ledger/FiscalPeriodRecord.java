package com.acme.finance.domains.ledger;



/**
 * An immutable persistence DTO. Snapshot bytes never escape by reference.
 */
public record FiscalPeriodRecord(String id, String label, byte[] snapshot) {
    public FiscalPeriodRecord {
        snapshot = snapshot.clone();
    }

    @Override
    public byte[] snapshot() {
        return snapshot.clone();
    }
}
