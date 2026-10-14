package com.acme.finance.domains.procurement;



/**
 * An immutable persistence DTO. Snapshot bytes never escape by reference.
 */
public record PurchaseOrderRecord(String id, String label, byte[] snapshot) {
    public PurchaseOrderRecord {
        snapshot = snapshot.clone();
    }

    @Override
    public byte[] snapshot() {
        return snapshot.clone();
    }
}
