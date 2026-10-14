package com.acme.finance.domains.insurance;

import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.Underwriting;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * JDBC adapter for the insurance domain. Tenant keys scope every write.
 */
@Repository
public class JdbcInsuranceRepository implements InsuranceRepository {
    private final JdbcTemplate jdbc;

    private final ClaimRecordMapper claimMapper;

    private final CoverageRecordMapper coverageMapper;

    private final PolicyRecordMapper policyMapper;

    private final PremiumRecordMapper premiumMapper;

    private final UnderwritingRecordMapper underwritingMapper;

    public JdbcInsuranceRepository(
            JdbcTemplate jdbc,
            ClaimRecordMapper claimMapper,
            CoverageRecordMapper coverageMapper,
            PolicyRecordMapper policyMapper,
            PremiumRecordMapper premiumMapper,
            UnderwritingRecordMapper underwritingMapper) {
        this.jdbc = jdbc;
        this.claimMapper = claimMapper;
        this.coverageMapper = coverageMapper;
        this.policyMapper = policyMapper;
        this.premiumMapper = premiumMapper;
        this.underwritingMapper = underwritingMapper;
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
    public void saveClaim(String tenant, Claim value) {
        var record = claimMapper.toRecord(value);
        save(tenant, "Claim", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveCoverage(String tenant, Coverage value) {
        var record = coverageMapper.toRecord(value);
        save(tenant, "Coverage", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void savePolicy(String tenant, Policy value) {
        var record = policyMapper.toRecord(value);
        save(tenant, "Policy", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void savePremium(String tenant, Premium value) {
        var record = premiumMapper.toRecord(value);
        save(tenant, "Premium", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveUnderwriting(String tenant, Underwriting value) {
        var record = underwritingMapper.toRecord(value);
        save(tenant, "Underwriting", record.id(), record.label(), record.snapshot());
    }
}
