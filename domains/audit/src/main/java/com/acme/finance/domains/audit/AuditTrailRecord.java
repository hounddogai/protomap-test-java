package com.acme.finance.domains.audit;



/**
 * An immutable persistence DTO. Snapshot bytes never escape by reference.
 */
public record AuditTrailRecord(String id, String label, byte[] snapshot) {
    public AuditTrailRecord {
        snapshot = snapshot.clone();
    }

    @Override
    public byte[] snapshot() {
        return snapshot.clone();
    }
}
