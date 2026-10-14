package com.acme.finance.domains.lending;



/**
 * An immutable persistence DTO. Snapshot bytes never escape by reference.
 */
public record LoanApplicationRecord(String id, String label, byte[] snapshot) {
    public LoanApplicationRecord {
        snapshot = snapshot.clone();
    }

    @Override
    public byte[] snapshot() {
        return snapshot.clone();
    }
}
