package com.acme.finance.domains.insurance;

import com.acme.insurance.v1.Policy;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface PolicyRecordMapper {
    PolicyRecord toRecord(Policy value);

    Policy toProto(PolicyRecord value);
}
