package com.acme.finance.domains.invoicing;



/**
 * An immutable persistence DTO. Snapshot bytes never escape by reference.
 */
public record EInvoiceRecord(String id, String label, byte[] snapshot) {
    public EInvoiceRecord {
        snapshot = snapshot.clone();
    }

    @Override
    public byte[] snapshot() {
        return snapshot.clone();
    }
}
