package com.acme.finance.domains.billing;

import com.acme.billing.v1.Dunning;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface DunningRecordMapper {
    DunningRecord toRecord(Dunning value);

    Dunning toProto(DunningRecord value);
}
