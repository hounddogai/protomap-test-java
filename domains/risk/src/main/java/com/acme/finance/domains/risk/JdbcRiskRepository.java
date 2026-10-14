package com.acme.finance.domains.risk;

import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.StressTest;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * JDBC adapter for the risk domain. Tenant keys scope every write.
 */
@Repository
public class JdbcRiskRepository implements RiskRepository {
    private final JdbcTemplate jdbc;

    private final ExposureRecordMapper exposureMapper;

    private final RiskRegisterRecordMapper riskRegisterMapper;

    private final RiskScenarioRecordMapper riskScenarioMapper;

    private final StressTestRecordMapper stressTestMapper;

    public JdbcRiskRepository(
            JdbcTemplate jdbc,
            ExposureRecordMapper exposureMapper,
            RiskRegisterRecordMapper riskRegisterMapper,
            RiskScenarioRecordMapper riskScenarioMapper,
            StressTestRecordMapper stressTestMapper) {
        this.jdbc = jdbc;
        this.exposureMapper = exposureMapper;
        this.riskRegisterMapper = riskRegisterMapper;
        this.riskScenarioMapper = riskScenarioMapper;
        this.stressTestMapper = stressTestMapper;
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
    public void saveExposure(String tenant, Exposure value) {
        var record = exposureMapper.toRecord(value);
        save(tenant, "Exposure", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveRiskRegister(String tenant, RiskRegister value) {
        var record = riskRegisterMapper.toRecord(value);
        save(tenant, "RiskRegister", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveRiskScenario(String tenant, RiskScenario value) {
        var record = riskScenarioMapper.toRecord(value);
        save(tenant, "RiskScenario", record.id(), record.label(), record.snapshot());
    }

    @Override
    public void saveStressTest(String tenant, StressTest value) {
        var record = stressTestMapper.toRecord(value);
        save(tenant, "StressTest", record.id(), record.label(), record.snapshot());
    }
}
