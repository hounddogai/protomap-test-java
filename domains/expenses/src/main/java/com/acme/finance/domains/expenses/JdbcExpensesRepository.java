package com.acme.finance.domains.expenses;

import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.Receipt;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * JDBC adapter for the expenses domain. Tenant keys scope every write.
 */
@Repository
public class JdbcExpensesRepository implements ExpensesRepository {
    private final JdbcTemplate jdbc;

    private final ExpensePolicyRecordMapper expensePolicyMapper;

    private final ExpenseReportRecordMapper expenseReportMapper;

    private final MileageClaimRecordMapper mileageClaimMapper;

    private final ReceiptRecordMapper receiptMapper;

    public JdbcExpensesRepository(
            JdbcTemplate jdbc,
            ExpensePolicyRecordMapper expensePolicyMapper,
            ExpenseReportRecordMapper expenseReportMapper,
            MileageClaimRecordMapper mileageClaimMapper,
            ReceiptRecordMapper receiptMapper) {
        this.jdbc = jdbc;
        this.expensePolicyMapper = expensePolicyMapper;
        this.expenseReportMapper = expenseReportMapper;
        this.mileageClaimMapper = mileageClaimMapper;
        this.receiptMapper = receiptMapper;
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
    public void saveExpensePolicy(String tenant, ExpensePolicy value) {
        var record = expensePolicyMapper.toRecord(value);
        save(tenant, "ExpensePolicy", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveExpenseReport(String tenant, ExpenseReport value) {
        var record = expenseReportMapper.toRecord(value);
        save(tenant, "ExpenseReport", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveMileageClaim(String tenant, MileageClaim value) {
        var record = mileageClaimMapper.toRecord(value);
        save(tenant, "MileageClaim", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveReceipt(String tenant, Receipt value) {
        var record = receiptMapper.toRecord(value);
        save(tenant, "Receipt", record.id(), record.label(), record.snapshot());
    }
}
