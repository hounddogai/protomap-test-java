package com.acme.finance.domains.treasury;



/**
 * An immutable persistence DTO. Snapshot bytes never escape by reference.
 */
public record CashPositionRecord(String id, String label, byte[] snapshot) {
    public CashPositionRecord {
        snapshot = snapshot.clone();
    }

    @Override
    public byte[] snapshot() {
        return snapshot.clone();
    }
}
