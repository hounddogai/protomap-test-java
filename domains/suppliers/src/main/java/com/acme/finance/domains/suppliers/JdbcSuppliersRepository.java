package com.acme.finance.domains.suppliers;

import com.acme.suppliers.v1.Supplier;
import com.acme.suppliers.v1.SupplierContract;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRisk;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * JDBC adapter for the suppliers domain. Tenant keys scope every write.
 */
@Repository
public class JdbcSuppliersRepository implements SuppliersRepository {
    private final JdbcTemplate jdbc;

    private final SupplierRecordMapper supplierMapper;

    private final SupplierContractRecordMapper supplierContractMapper;

    private final SupplierRatingRecordMapper supplierRatingMapper;

    private final SupplierRiskRecordMapper supplierRiskMapper;

    public JdbcSuppliersRepository(
            JdbcTemplate jdbc,
            SupplierRecordMapper supplierMapper,
            SupplierContractRecordMapper supplierContractMapper,
            SupplierRatingRecordMapper supplierRatingMapper,
            SupplierRiskRecordMapper supplierRiskMapper) {
        this.jdbc = jdbc;
        this.supplierMapper = supplierMapper;
        this.supplierContractMapper = supplierContractMapper;
        this.supplierRatingMapper = supplierRatingMapper;
        this.supplierRiskMapper = supplierRiskMapper;
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
    public void saveSupplier(String tenant, Supplier value) {
        var record = supplierMapper.toRecord(value);
        save(tenant, "Supplier", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveSupplierContract(String tenant, SupplierContract value) {
        var record = supplierContractMapper.toRecord(value);
        save(tenant, "SupplierContract", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveSupplierRating(String tenant, SupplierRating value) {
        var record = supplierRatingMapper.toRecord(value);
        save(tenant, "SupplierRating", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveSupplierRisk(String tenant, SupplierRisk value) {
        var record = supplierRiskMapper.toRecord(value);
        save(tenant, "SupplierRisk", record.id(), record.label(), record.snapshot());
    }
}
