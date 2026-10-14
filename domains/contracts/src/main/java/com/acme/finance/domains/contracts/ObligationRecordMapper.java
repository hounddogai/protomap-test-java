package com.acme.finance.domains.contracts;

import com.acme.contracts.v1.Obligation;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface ObligationRecordMapper {
    ObligationRecord toRecord(Obligation value);

    Obligation toProto(ObligationRecord value);
}
