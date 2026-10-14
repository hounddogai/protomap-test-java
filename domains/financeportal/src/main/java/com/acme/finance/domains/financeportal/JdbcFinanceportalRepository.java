package com.acme.finance.domains.financeportal;

import com.acme.financeportal.v1.ApprovalInbox;
import com.acme.financeportal.v1.CloseChecklist;
import com.acme.financeportal.v1.FinanceWorkspace;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * JDBC adapter for the financeportal domain. Tenant keys scope every write.
 */
@Repository
public class JdbcFinanceportalRepository implements FinanceportalRepository {
    private final JdbcTemplate jdbc;

    private final ApprovalInboxRecordMapper approvalInboxMapper;

    private final CloseChecklistRecordMapper closeChecklistMapper;

    private final FinanceWorkspaceRecordMapper financeWorkspaceMapper;

    public JdbcFinanceportalRepository(
            JdbcTemplate jdbc,
            ApprovalInboxRecordMapper approvalInboxMapper,
            CloseChecklistRecordMapper closeChecklistMapper,
            FinanceWorkspaceRecordMapper financeWorkspaceMapper) {
        this.jdbc = jdbc;
        this.approvalInboxMapper = approvalInboxMapper;
        this.closeChecklistMapper = closeChecklistMapper;
        this.financeWorkspaceMapper = financeWorkspaceMapper;
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
    public void saveApprovalInbox(String tenant, ApprovalInbox value) {
        var record = approvalInboxMapper.toRecord(value);
        save(tenant, "ApprovalInbox", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveCloseChecklist(String tenant, CloseChecklist value) {
        var record = closeChecklistMapper.toRecord(value);
        save(tenant, "CloseChecklist", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveFinanceWorkspace(String tenant, FinanceWorkspace value) {
        var record = financeWorkspaceMapper.toRecord(value);
        save(tenant, "FinanceWorkspace", record.id(), record.label(), record.snapshot());
    }
}
