package com.acme.finance.domains.ledger;



/**
 * An immutable persistence DTO. Snapshot bytes never escape by reference.
 */
public record JournalEntryRecord(String id, String label, byte[] snapshot) {
    public JournalEntryRecord {
        snapshot = snapshot.clone();
    }

    @Override
    public byte[] snapshot() {
        return snapshot.clone();
    }
}
