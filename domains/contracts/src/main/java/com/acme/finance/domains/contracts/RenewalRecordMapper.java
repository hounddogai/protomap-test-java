package com.acme.finance.domains.contracts;

import com.acme.contracts.v1.Renewal;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface RenewalRecordMapper {
    RenewalRecord toRecord(Renewal value);

    Renewal toProto(RenewalRecord value);
}
