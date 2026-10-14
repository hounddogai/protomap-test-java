package com.acme.finance.domains.contracts;

import com.acme.contracts.v1.Contract;
import com.acme.contracts.v1.ContractClause;
import com.acme.contracts.v1.Obligation;
import com.acme.contracts.v1.Renewal;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * JDBC adapter for the contracts domain. Tenant keys scope every write.
 */
@Repository
public class JdbcContractsRepository implements ContractsRepository {
    private final JdbcTemplate jdbc;

    private final ContractRecordMapper contractMapper;

    private final ContractClauseRecordMapper contractClauseMapper;

    private final ObligationRecordMapper obligationMapper;

    private final RenewalRecordMapper renewalMapper;

    public JdbcContractsRepository(
            JdbcTemplate jdbc,
            ContractRecordMapper contractMapper,
            ContractClauseRecordMapper contractClauseMapper,
            ObligationRecordMapper obligationMapper,
            RenewalRecordMapper renewalMapper) {
        this.jdbc = jdbc;
        this.contractMapper = contractMapper;
        this.contractClauseMapper = contractClauseMapper;
        this.obligationMapper = obligationMapper;
        this.renewalMapper = renewalMapper;
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
    public void saveContract(String tenant, Contract value) {
        var record = contractMapper.toRecord(value);
        save(tenant, "Contract", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveContractClause(String tenant, ContractClause value) {
        var record = contractClauseMapper.toRecord(value);
        save(tenant, "ContractClause", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveObligation(String tenant, Obligation value) {
        var record = obligationMapper.toRecord(value);
        save(tenant, "Obligation", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveRenewal(String tenant, Renewal value) {
        var record = renewalMapper.toRecord(value);
        save(tenant, "Renewal", record.id(), record.label(), record.snapshot());
    }
}
