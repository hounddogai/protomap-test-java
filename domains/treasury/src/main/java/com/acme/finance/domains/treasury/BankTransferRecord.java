package com.acme.finance.domains.treasury;



/**
 * An immutable persistence DTO. Snapshot bytes never escape by reference.
 */
public record BankTransferRecord(String id, String label, byte[] snapshot) {
    public BankTransferRecord {
        snapshot = snapshot.clone();
    }

    @Override
    public byte[] snapshot() {
        return snapshot.clone();
    }
}
