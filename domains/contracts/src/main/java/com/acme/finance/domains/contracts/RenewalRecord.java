package com.acme.finance.domains.contracts;



/**
 * An immutable persistence DTO. Snapshot bytes never escape by reference.
 */
public record RenewalRecord(String id, String label, byte[] snapshot) {
    public RenewalRecord {
        snapshot = snapshot.clone();
    }

    @Override
    public byte[] snapshot() {
        return snapshot.clone();
    }
}
