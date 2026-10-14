package com.acme.finance.domains.banking;



/**
 * An immutable persistence DTO. Snapshot bytes never escape by reference.
 */
public record StandingOrderRecord(String id, String label, byte[] snapshot) {
    public StandingOrderRecord {
        snapshot = snapshot.clone();
    }

    @Override
    public byte[] snapshot() {
        return snapshot.clone();
    }
}
