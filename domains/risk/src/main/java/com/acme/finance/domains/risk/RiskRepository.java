package com.acme.finance.domains.risk;

import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.StressTest;

/**
 * The domain's durable record and audit port.
 */
public interface RiskRepository {
    void recordAction(String tenant, String kind, String id, String action);

    void recordDetail(String tenant, String operation, String detail);

    void saveExposure(String tenant, Exposure value);

    void saveRiskRegister(String tenant, RiskRegister value);

    void saveRiskScenario(String tenant, RiskScenario value);

    void saveStressTest(String tenant, StressTest value);
}
