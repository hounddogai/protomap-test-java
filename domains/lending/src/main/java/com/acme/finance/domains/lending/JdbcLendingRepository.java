package com.acme.finance.domains.lending;

import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.Repayment;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * JDBC adapter for the lending domain. Tenant keys scope every write.
 */
@Repository
public class JdbcLendingRepository implements LendingRepository {
    private final JdbcTemplate jdbc;

    private final CollateralRecordMapper collateralMapper;

    private final LoanRecordMapper loanMapper;

    private final LoanApplicationRecordMapper loanApplicationMapper;

    private final RepaymentRecordMapper repaymentMapper;

    public JdbcLendingRepository(
            JdbcTemplate jdbc,
            CollateralRecordMapper collateralMapper,
            LoanRecordMapper loanMapper,
            LoanApplicationRecordMapper loanApplicationMapper,
            RepaymentRecordMapper repaymentMapper) {
        this.jdbc = jdbc;
        this.collateralMapper = collateralMapper;
        this.loanMapper = loanMapper;
        this.loanApplicationMapper = loanApplicationMapper;
        this.repaymentMapper = repaymentMapper;
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
    public void saveCollateral(String tenant, Collateral value) {
        var record = collateralMapper.toRecord(value);
        save(tenant, "Collateral", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveLoan(String tenant, Loan value) {
        var record = loanMapper.toRecord(value);
        save(tenant, "Loan", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveLoanApplication(String tenant, LoanApplication value) {
        var record = loanApplicationMapper.toRecord(value);
        save(tenant, "LoanApplication", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveRepayment(String tenant, Repayment value) {
        var record = repaymentMapper.toRecord(value);
        save(tenant, "Repayment", record.id(), record.label(), record.snapshot());
    }
}
