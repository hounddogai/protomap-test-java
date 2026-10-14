package com.acme.finance.domains.suppliers;



/**
 * An immutable persistence DTO. Snapshot bytes never escape by reference.
 */
public record SupplierRecord(String id, String label, byte[] snapshot) {
    public SupplierRecord {
        snapshot = snapshot.clone();
    }

    @Override
    public byte[] snapshot() {
        return snapshot.clone();
    }
}
