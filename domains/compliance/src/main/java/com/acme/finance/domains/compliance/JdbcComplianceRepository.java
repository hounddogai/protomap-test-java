package com.acme.finance.domains.compliance;

import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.Sanction;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * JDBC adapter for the compliance domain. Tenant keys scope every write.
 */
@Repository
public class JdbcComplianceRepository implements ComplianceRepository {
    private final JdbcTemplate jdbc;

    private final AttestationRecordMapper attestationMapper;

    private final ComplianceCaseRecordMapper complianceCaseMapper;

    private final ControlRecordMapper controlMapper;

    private final SanctionRecordMapper sanctionMapper;

    public JdbcComplianceRepository(
            JdbcTemplate jdbc,
            AttestationRecordMapper attestationMapper,
            ComplianceCaseRecordMapper complianceCaseMapper,
            ControlRecordMapper controlMapper,
            SanctionRecordMapper sanctionMapper) {
        this.jdbc = jdbc;
        this.attestationMapper = attestationMapper;
        this.complianceCaseMapper = complianceCaseMapper;
        this.controlMapper = controlMapper;
        this.sanctionMapper = sanctionMapper;
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
    public void saveAttestation(String tenant, Attestation value) {
        var record = attestationMapper.toRecord(value);
        save(tenant, "Attestation", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveComplianceCase(String tenant, ComplianceCase value) {
        var record = complianceCaseMapper.toRecord(value);
        save(tenant, "ComplianceCase", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveControl(String tenant, Control value) {
        var record = controlMapper.toRecord(value);
        save(tenant, "Control", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveSanction(String tenant, Sanction value) {
        var record = sanctionMapper.toRecord(value);
        save(tenant, "Sanction", record.id(), record.label(), record.snapshot());
    }
}
