package com.acme.finance.domains.accounting;



/**
 * An immutable persistence DTO. Snapshot bytes never escape by reference.
 */
public record ChartOfAccountsRecord(String id, String label, byte[] snapshot) {
    public ChartOfAccountsRecord {
        snapshot = snapshot.clone();
    }

    @Override
    public byte[] snapshot() {
        return snapshot.clone();
    }
}
