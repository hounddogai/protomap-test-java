package com.acme.finance.domains.insurance;

import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.Underwriting;

/**
 * The domain's durable record and audit port.
 */
public interface InsuranceRepository {
    void recordAction(String tenant, String kind, String id, String action);

    void recordDetail(String tenant, String operation, String detail);

    void saveClaim(String tenant, Claim value);

    void saveCoverage(String tenant, Coverage value);

    void savePolicy(String tenant, Policy value);

    void savePremium(String tenant, Premium value);

    void saveUnderwriting(String tenant, Underwriting value);
}
