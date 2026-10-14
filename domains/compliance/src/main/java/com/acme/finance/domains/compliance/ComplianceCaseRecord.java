package com.acme.finance.domains.compliance;



/**
 * An immutable persistence DTO. Snapshot bytes never escape by reference.
 */
public record ComplianceCaseRecord(String id, String label, byte[] snapshot) {
    public ComplianceCaseRecord {
        snapshot = snapshot.clone();
    }

    @Override
    public byte[] snapshot() {
        return snapshot.clone();
    }
}
