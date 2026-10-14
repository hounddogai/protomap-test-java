package com.acme.finance.domains.payroll;

import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.TaxWithholding;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * JDBC adapter for the payroll domain. Tenant keys scope every write.
 */
@Repository
public class JdbcPayrollRepository implements PayrollRepository {
    private final JdbcTemplate jdbc;

    private final DeductionRecordMapper deductionMapper;

    private final PayRunRecordMapper payRunMapper;

    private final PayStubRecordMapper payStubMapper;

    private final TaxWithholdingRecordMapper taxWithholdingMapper;

    public JdbcPayrollRepository(
            JdbcTemplate jdbc,
            DeductionRecordMapper deductionMapper,
            PayRunRecordMapper payRunMapper,
            PayStubRecordMapper payStubMapper,
            TaxWithholdingRecordMapper taxWithholdingMapper) {
        this.jdbc = jdbc;
        this.deductionMapper = deductionMapper;
        this.payRunMapper = payRunMapper;
        this.payStubMapper = payStubMapper;
        this.taxWithholdingMapper = taxWithholdingMapper;
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
    public void saveDeduction(String tenant, Deduction value) {
        var record = deductionMapper.toRecord(value);
        save(tenant, "Deduction", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void savePayRun(String tenant, PayRun value) {
        var record = payRunMapper.toRecord(value);
        save(tenant, "PayRun", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void savePayStub(String tenant, PayStub value) {
        var record = payStubMapper.toRecord(value);
        save(tenant, "PayStub", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveTaxWithholding(String tenant, TaxWithholding value) {
        var record = taxWithholdingMapper.toRecord(value);
        save(tenant, "TaxWithholding", record.id(), record.label(), record.snapshot());
    }
}
