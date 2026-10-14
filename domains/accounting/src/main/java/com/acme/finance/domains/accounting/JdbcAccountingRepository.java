package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.Accrual;
import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.ChartOfAccounts;
import com.acme.accounting.v1.Reconciliation;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * JDBC adapter for the accounting domain. Tenant keys scope every write.
 */
@Repository
public class JdbcAccountingRepository implements AccountingRepository {
    private final JdbcTemplate jdbc;

    private final AccrualRecordMapper accrualMapper;

    private final AllocationRecordMapper allocationMapper;

    private final ChartOfAccountsRecordMapper chartOfAccountsMapper;

    private final ReconciliationRecordMapper reconciliationMapper;

    public JdbcAccountingRepository(
            JdbcTemplate jdbc,
            AccrualRecordMapper accrualMapper,
            AllocationRecordMapper allocationMapper,
            ChartOfAccountsRecordMapper chartOfAccountsMapper,
            ReconciliationRecordMapper reconciliationMapper) {
        this.jdbc = jdbc;
        this.accrualMapper = accrualMapper;
        this.allocationMapper = allocationMapper;
        this.chartOfAccountsMapper = chartOfAccountsMapper;
        this.reconciliationMapper = reconciliationMapper;
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
    public void saveAccrual(String tenant, Accrual value) {
        var record = accrualMapper.toRecord(value);
        save(tenant, "Accrual", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveAllocation(String tenant, Allocation value) {
        var record = allocationMapper.toRecord(value);
        save(tenant, "Allocation", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveChartOfAccounts(String tenant, ChartOfAccounts value) {
        var record = chartOfAccountsMapper.toRecord(value);
        save(tenant, "ChartOfAccounts", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveReconciliation(String tenant, Reconciliation value) {
        var record = reconciliationMapper.toRecord(value);
        save(tenant, "Reconciliation", record.id(), record.label(), record.snapshot());
    }
}
