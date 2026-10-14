package com.acme.finance.domains.risk;

import com.acme.risk.v1.StressTest;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface StressTestRecordMapper {
    StressTestRecord toRecord(StressTest value);

    StressTest toProto(StressTestRecord value);
}
