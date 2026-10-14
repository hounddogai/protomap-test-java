package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.Variance;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * JDBC adapter for the budgeting domain. Tenant keys scope every write.
 */
@Repository
public class JdbcBudgetingRepository implements BudgetingRepository {
    private final JdbcTemplate jdbc;

    private final BudgetRecordMapper budgetMapper;

    private final BudgetLineRecordMapper budgetLineMapper;

    private final ForecastRecordMapper forecastMapper;

    private final VarianceRecordMapper varianceMapper;

    public JdbcBudgetingRepository(
            JdbcTemplate jdbc,
            BudgetRecordMapper budgetMapper,
            BudgetLineRecordMapper budgetLineMapper,
            ForecastRecordMapper forecastMapper,
            VarianceRecordMapper varianceMapper) {
        this.jdbc = jdbc;
        this.budgetMapper = budgetMapper;
        this.budgetLineMapper = budgetLineMapper;
        this.forecastMapper = forecastMapper;
        this.varianceMapper = varianceMapper;
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
    public void saveBudget(String tenant, Budget value) {
        var record = budgetMapper.toRecord(value);
        save(tenant, "Budget", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveBudgetLine(String tenant, BudgetLine value) {
        var record = budgetLineMapper.toRecord(value);
        save(tenant, "BudgetLine", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveForecast(String tenant, Forecast value) {
        var record = forecastMapper.toRecord(value);
        save(tenant, "Forecast", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveVariance(String tenant, Variance value) {
        var record = varianceMapper.toRecord(value);
        save(tenant, "Variance", record.id(), record.label(), record.snapshot());
    }
}
