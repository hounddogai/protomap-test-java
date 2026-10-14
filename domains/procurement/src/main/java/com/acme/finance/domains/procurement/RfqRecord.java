package com.acme.finance.domains.procurement;



/**
 * An immutable persistence DTO. Snapshot bytes never escape by reference.
 */
public record RfqRecord(String id, String label, byte[] snapshot) {
    public RfqRecord {
        snapshot = snapshot.clone();
    }

    @Override
    public byte[] snapshot() {
        return snapshot.clone();
    }
}
