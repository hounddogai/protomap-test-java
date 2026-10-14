package com.acme.finance.domains.banking;

import com.acme.banking.v1.Mandate;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface MandateRecordMapper {
    MandateRecord toRecord(Mandate value);

    Mandate toProto(MandateRecord value);
}
