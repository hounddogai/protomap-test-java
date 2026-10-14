package com.acme.finance.domains.invoicing;



/**
 * An immutable persistence DTO. Snapshot bytes never escape by reference.
 */
public record InvoiceBatchRecord(String id, String label, byte[] snapshot) {
    public InvoiceBatchRecord {
        snapshot = snapshot.clone();
    }

    @Override
    public byte[] snapshot() {
        return snapshot.clone();
    }
}
