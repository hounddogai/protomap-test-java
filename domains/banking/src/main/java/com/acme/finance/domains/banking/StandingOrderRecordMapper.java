package com.acme.finance.domains.banking;

import com.acme.banking.v1.StandingOrder;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface StandingOrderRecordMapper {
    StandingOrderRecord toRecord(StandingOrder value);

    StandingOrder toProto(StandingOrderRecord value);
}
