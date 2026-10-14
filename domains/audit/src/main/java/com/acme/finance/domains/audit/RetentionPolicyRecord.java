package com.acme.finance.domains.audit;



/**
 * An immutable persistence DTO. Snapshot bytes never escape by reference.
 */
public record RetentionPolicyRecord(String id, String label, byte[] snapshot) {
    public RetentionPolicyRecord {
        snapshot = snapshot.clone();
    }

    @Override
    public byte[] snapshot() {
        return snapshot.clone();
    }
}
