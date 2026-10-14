package com.acme.finance.domains.insurance;

import com.acme.insurance.v1.Coverage;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface CoverageRecordMapper {
    CoverageRecord toRecord(Coverage value);

    Coverage toProto(CoverageRecord value);
}
