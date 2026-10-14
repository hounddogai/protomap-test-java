package com.acme.finance.domains.treasury;

import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.LiquidityForecast;

/**
 * The domain's durable record and audit port.
 */
public interface TreasuryRepository {
    void recordAction(String tenant, String kind, String id, String action);

    void recordDetail(String tenant, String operation, String detail);

    void saveBankTransfer(String tenant, BankTransfer value);

    void saveCashPosition(String tenant, CashPosition value);

    void saveFxHedge(String tenant, FxHedge value);

    void saveLiquidityForecast(String tenant, LiquidityForecast value);
}
