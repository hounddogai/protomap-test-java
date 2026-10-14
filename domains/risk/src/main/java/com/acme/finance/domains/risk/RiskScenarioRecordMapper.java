package com.acme.finance.domains.risk;

import com.acme.risk.v1.RiskScenario;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface RiskScenarioRecordMapper {
    RiskScenarioRecord toRecord(RiskScenario value);

    RiskScenario toProto(RiskScenarioRecord value);
}
