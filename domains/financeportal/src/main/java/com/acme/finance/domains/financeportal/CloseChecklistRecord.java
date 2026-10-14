package com.acme.finance.domains.financeportal;



/**
 * An immutable persistence DTO. Snapshot bytes never escape by reference.
 */
public record CloseChecklistRecord(String id, String label, byte[] snapshot) {
    public CloseChecklistRecord {
        snapshot = snapshot.clone();
    }

    @Override
    public byte[] snapshot() {
        return snapshot.clone();
    }
}
