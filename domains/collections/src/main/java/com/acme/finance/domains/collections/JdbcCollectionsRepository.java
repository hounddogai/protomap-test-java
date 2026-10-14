package com.acme.finance.domains.collections;

import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.WriteOff;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * JDBC adapter for the collections domain. Tenant keys scope every write.
 */
@Repository
public class JdbcCollectionsRepository implements CollectionsRepository {
    private final JdbcTemplate jdbc;

    private final CollectionCaseRecordMapper collectionCaseMapper;

    private final PaymentPromiseRecordMapper paymentPromiseMapper;

    private final WriteOffRecordMapper writeOffMapper;

    public JdbcCollectionsRepository(
            JdbcTemplate jdbc,
            CollectionCaseRecordMapper collectionCaseMapper,
            PaymentPromiseRecordMapper paymentPromiseMapper,
            WriteOffRecordMapper writeOffMapper) {
        this.jdbc = jdbc;
        this.collectionCaseMapper = collectionCaseMapper;
        this.paymentPromiseMapper = paymentPromiseMapper;
        this.writeOffMapper = writeOffMapper;
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
    public void saveCollectionCase(String tenant, CollectionCase value) {
        var record = collectionCaseMapper.toRecord(value);
        save(tenant, "CollectionCase", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void savePaymentPromise(String tenant, PaymentPromise value) {
        var record = paymentPromiseMapper.toRecord(value);
        save(tenant, "PaymentPromise", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveWriteOff(String tenant, WriteOff value) {
        var record = writeOffMapper.toRecord(value);
        save(tenant, "WriteOff", record.id(), record.label(), record.snapshot());
    }
}
