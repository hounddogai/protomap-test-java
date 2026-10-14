package com.acme.finance.domains.compliance;



/**
 * An immutable persistence DTO. Snapshot bytes never escape by reference.
 */
public record SanctionRecord(String id, String label, byte[] snapshot) {
    public SanctionRecord {
        snapshot = snapshot.clone();
    }

    @Override
    public byte[] snapshot() {
        return snapshot.clone();
    }
}
