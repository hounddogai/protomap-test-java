package com.acme.finance.domains.compliance;

import com.acme.compliance.v1.Sanction;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface SanctionRecordMapper {
    SanctionRecord toRecord(Sanction value);

    Sanction toProto(SanctionRecord value);
}
