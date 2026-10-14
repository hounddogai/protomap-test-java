package com.acme.finance.domains.suppliers;



/**
 * An immutable persistence DTO. Snapshot bytes never escape by reference.
 */
public record SupplierRatingRecord(String id, String label, byte[] snapshot) {
    public SupplierRatingRecord {
        snapshot = snapshot.clone();
    }

    @Override
    public byte[] snapshot() {
        return snapshot.clone();
    }
}
