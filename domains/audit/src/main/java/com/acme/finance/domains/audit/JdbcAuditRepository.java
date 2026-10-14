package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.RetentionPolicy;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * JDBC adapter for the audit domain. Tenant keys scope every write.
 */
@Repository
public class JdbcAuditRepository implements AuditRepository {
    private final JdbcTemplate jdbc;

    private final AuditEventRecordMapper auditEventMapper;

    private final AuditTrailRecordMapper auditTrailMapper;

    private final RetentionPolicyRecordMapper retentionPolicyMapper;

    public JdbcAuditRepository(
            JdbcTemplate jdbc,
            AuditEventRecordMapper auditEventMapper,
            AuditTrailRecordMapper auditTrailMapper,
            RetentionPolicyRecordMapper retentionPolicyMapper) {
        this.jdbc = jdbc;
        this.auditEventMapper = auditEventMapper;
        this.auditTrailMapper = auditTrailMapper;
        this.retentionPolicyMapper = retentionPolicyMapper;
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
    public void saveAuditEvent(String tenant, AuditEvent value) {
        var record = auditEventMapper.toRecord(value);
        save(tenant, "AuditEvent", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveAuditTrail(String tenant, AuditTrail value) {
        var record = auditTrailMapper.toRecord(value);
        save(tenant, "AuditTrail", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveRetentionPolicy(String tenant, RetentionPolicy value) {
        var record = retentionPolicyMapper.toRecord(value);
        save(tenant, "RetentionPolicy", record.id(), record.label(), record.snapshot());
    }
}
