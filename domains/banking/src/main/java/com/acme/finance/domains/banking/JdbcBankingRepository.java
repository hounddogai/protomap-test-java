package com.acme.finance.domains.banking;

import com.acme.banking.v1.BankAccount;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.Statement;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * JDBC adapter for the banking domain. Tenant keys scope every write.
 */
@Repository
public class JdbcBankingRepository implements BankingRepository {
    private final JdbcTemplate jdbc;

    private final BankAccountRecordMapper bankAccountMapper;

    private final MandateRecordMapper mandateMapper;

    private final StandingOrderRecordMapper standingOrderMapper;

    private final StatementRecordMapper statementMapper;

    public JdbcBankingRepository(
            JdbcTemplate jdbc,
            BankAccountRecordMapper bankAccountMapper,
            MandateRecordMapper mandateMapper,
            StandingOrderRecordMapper standingOrderMapper,
            StatementRecordMapper statementMapper) {
        this.jdbc = jdbc;
        this.bankAccountMapper = bankAccountMapper;
        this.mandateMapper = mandateMapper;
        this.standingOrderMapper = standingOrderMapper;
        this.statementMapper = statementMapper;
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
    public void saveBankAccount(String tenant, BankAccount value) {
        var record = bankAccountMapper.toRecord(value);
        save(tenant, "BankAccount", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveMandate(String tenant, Mandate value) {
        var record = mandateMapper.toRecord(value);
        save(tenant, "Mandate", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveStandingOrder(String tenant, StandingOrder value) {
        var record = standingOrderMapper.toRecord(value);
        save(tenant, "StandingOrder", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveStatement(String tenant, Statement value) {
        var record = statementMapper.toRecord(value);
        save(tenant, "Statement", record.id(), record.label(), record.snapshot());
    }
}
