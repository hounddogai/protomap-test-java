package com.acme.finance.domains.ledger;

import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.PostingBatch;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * JDBC adapter for the ledger domain. Tenant keys scope every write.
 */
@Repository
public class JdbcLedgerRepository implements LedgerRepository {
    private final JdbcTemplate jdbc;

    private final FiscalPeriodRecordMapper fiscalPeriodMapper;

    private final JournalEntryRecordMapper journalEntryMapper;

    private final LedgerAccountRecordMapper ledgerAccountMapper;

    private final PostingBatchRecordMapper postingBatchMapper;

    public JdbcLedgerRepository(
            JdbcTemplate jdbc,
            FiscalPeriodRecordMapper fiscalPeriodMapper,
            JournalEntryRecordMapper journalEntryMapper,
            LedgerAccountRecordMapper ledgerAccountMapper,
            PostingBatchRecordMapper postingBatchMapper) {
        this.jdbc = jdbc;
        this.fiscalPeriodMapper = fiscalPeriodMapper;
        this.journalEntryMapper = journalEntryMapper;
        this.ledgerAccountMapper = ledgerAccountMapper;
        this.postingBatchMapper = postingBatchMapper;
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
    public void saveFiscalPeriod(String tenant, FiscalPeriod value) {
        var record = fiscalPeriodMapper.toRecord(value);
        save(tenant, "FiscalPeriod", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveJournalEntry(String tenant, JournalEntry value) {
        var record = journalEntryMapper.toRecord(value);
        save(tenant, "JournalEntry", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveLedgerAccount(String tenant, LedgerAccount value) {
        var record = ledgerAccountMapper.toRecord(value);
        save(tenant, "LedgerAccount", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void savePostingBatch(String tenant, PostingBatch value) {
        var record = postingBatchMapper.toRecord(value);
        save(tenant, "PostingBatch", record.id(), record.label(), record.snapshot());
    }
}
