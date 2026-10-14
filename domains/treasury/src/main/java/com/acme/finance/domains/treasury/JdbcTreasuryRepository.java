package com.acme.finance.domains.treasury;

import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.LiquidityForecast;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * JDBC adapter for the treasury domain. Tenant keys scope every write.
 */
@Repository
public class JdbcTreasuryRepository implements TreasuryRepository {
    private final JdbcTemplate jdbc;

    private final BankTransferRecordMapper bankTransferMapper;

    private final CashPositionRecordMapper cashPositionMapper;

    private final FxHedgeRecordMapper fxHedgeMapper;

    private final LiquidityForecastRecordMapper liquidityForecastMapper;

    public JdbcTreasuryRepository(
            JdbcTemplate jdbc,
            BankTransferRecordMapper bankTransferMapper,
            CashPositionRecordMapper cashPositionMapper,
            FxHedgeRecordMapper fxHedgeMapper,
            LiquidityForecastRecordMapper liquidityForecastMapper) {
        this.jdbc = jdbc;
        this.bankTransferMapper = bankTransferMapper;
        this.cashPositionMapper = cashPositionMapper;
        this.fxHedgeMapper = fxHedgeMapper;
        this.liquidityForecastMapper = liquidityForecastMapper;
    }

    private void save(String tenant, String kind, String id, String label, byte[] snapshot) {
        if (id.isBlank()) {
            throw new IllegalArgumentException("record identifier is required");
        }
        int updated = jdbc.update("UPDATE finance_records SET label = ?, snapshot = ? "
                + "WHERE tenant_id = ? AND entity_kind = ? AND record_id = ?", label, snapshot, tenant, kind, id);
        if (updated == 0) {
            try {
                jdbc.update("INSERT INTO finance_records (tenant_id, entity_kind, record_id, label, snapshot) "
                        + "VALUES (?, ?, ?, ?, ?)", tenant, kind, id, label, snapshot);
            } catch (DuplicateKeyException concurrentInsert) {
                jdbc.update("UPDATE finance_records SET label = ?, snapshot = ? "
                        + "WHERE tenant_id = ? AND entity_kind = ? AND record_id = ?", label, snapshot, tenant, kind, id);
            }
        }
    }

    @Override
    public void recordAction(String tenant, String kind, String id, String action) {
        jdbc.update("INSERT INTO finance_actions (tenant_id, entity_kind, record_id, operation) VALUES (?, ?, ?, ?)",
                tenant, kind, id, action);
    }

    @Override
    public void recordDetail(String tenant, String operation, String detail) {
        jdbc.update("INSERT INTO finance_details (tenant_id, operation, detail) VALUES (?, ?, ?)", tenant, operation, detail);
    }

    @Override
    public void saveBankTransfer(String tenant, BankTransfer value) {
        var record = bankTransferMapper.toRecord(value);
        save(tenant, "BankTransfer", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveCashPosition(String tenant, CashPosition value) {
        var record = cashPositionMapper.toRecord(value);
        save(tenant, "CashPosition", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveFxHedge(String tenant, FxHedge value) {
        var record = fxHedgeMapper.toRecord(value);
        save(tenant, "FxHedge", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveLiquidityForecast(String tenant, LiquidityForecast value) {
        var record = liquidityForecastMapper.toRecord(value);
        save(tenant, "LiquidityForecast", record.id(), record.label(), record.snapshot());
    }
}
