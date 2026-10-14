package com.acme.finance.domains.risk;

import com.acme.risk.v1.RiskRegister;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class RiskRegisterRecordMapperImpl implements RiskRegisterRecordMapper {
    @Override
    public RiskRegisterRecord toRecord(RiskRegister value) {
        return new RiskRegisterRecord(value.getRiskRegisterId(), value.getName(), value.toByteArray());
    }

    @Override
    public RiskRegister toProto(RiskRegisterRecord value) {
        try {
            return RiskRegister.parseFrom(value.snapshot()).toBuilder()
                    .setRiskRegisterId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
