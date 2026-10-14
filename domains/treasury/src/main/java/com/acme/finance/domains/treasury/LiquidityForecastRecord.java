package com.acme.finance.domains.treasury;



/**
 * An immutable persistence DTO. Snapshot bytes never escape by reference.
 */
public record LiquidityForecastRecord(String id, String label, byte[] snapshot) {
    public LiquidityForecastRecord {
        snapshot = snapshot.clone();
    }

    @Override
    public byte[] snapshot() {
        return snapshot.clone();
    }
}
