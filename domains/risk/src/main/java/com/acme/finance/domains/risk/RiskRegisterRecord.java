package com.acme.finance.domains.risk;



/**
 * An immutable persistence DTO. Snapshot bytes never escape by reference.
 */
public record RiskRegisterRecord(String id, String label, byte[] snapshot) {
    public RiskRegisterRecord {
        snapshot = snapshot.clone();
    }

    @Override
    public byte[] snapshot() {
        return snapshot.clone();
    }
}
