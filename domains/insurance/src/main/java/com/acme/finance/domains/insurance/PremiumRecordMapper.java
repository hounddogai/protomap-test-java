package com.acme.finance.domains.insurance;

import com.acme.insurance.v1.Premium;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface PremiumRecordMapper {
    PremiumRecord toRecord(Premium value);

    Premium toProto(PremiumRecord value);
}
