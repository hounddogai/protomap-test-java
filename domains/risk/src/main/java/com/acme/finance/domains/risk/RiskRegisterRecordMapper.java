package com.acme.finance.domains.risk;

import com.acme.risk.v1.RiskRegister;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface RiskRegisterRecordMapper {
    RiskRegisterRecord toRecord(RiskRegister value);

    RiskRegister toProto(RiskRegisterRecord value);
}
