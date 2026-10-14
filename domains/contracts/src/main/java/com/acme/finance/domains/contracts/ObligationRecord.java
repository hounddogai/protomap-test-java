package com.acme.finance.domains.contracts;



/**
 * An immutable persistence DTO. Snapshot bytes never escape by reference.
 */
public record ObligationRecord(String id, String label, byte[] snapshot) {
    public ObligationRecord {
        snapshot = snapshot.clone();
    }

    @Override
    public byte[] snapshot() {
        return snapshot.clone();
    }
}
