package com.acme.finance.domains.insurance;

import com.acme.insurance.v1.Underwriting;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface UnderwritingRecordMapper {
    UnderwritingRecord toRecord(Underwriting value);

    Underwriting toProto(UnderwritingRecord value);
}
