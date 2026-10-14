package com.acme.finance.domains.billing;



/**
 * An immutable persistence DTO. Snapshot bytes never escape by reference.
 */
public record DunningRecord(String id, String label, byte[] snapshot) {
    public DunningRecord {
        snapshot = snapshot.clone();
    }

    @Override
    public byte[] snapshot() {
        return snapshot.clone();
    }
}
