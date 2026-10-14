package com.acme.finance.domains.risk;

import com.acme.risk.v1.RiskScenario;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class RiskScenarioRecordMapperImpl implements RiskScenarioRecordMapper {
    @Override
    public RiskScenarioRecord toRecord(RiskScenario value) {
        return new RiskScenarioRecord(value.getRiskScenarioId(), value.getSubject(), value.toByteArray());
    }

    @Override
    public RiskScenario toProto(RiskScenarioRecord value) {
        try {
            return RiskScenario.parseFrom(value.snapshot()).toBuilder()
                    .setRiskScenarioId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
