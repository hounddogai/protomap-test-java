package com.acme.finance.domains.expenses;

import com.acme.expenses.v1.MileageClaim;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface MileageClaimRecordMapper {
    MileageClaimRecord toRecord(MileageClaim value);

    MileageClaim toProto(MileageClaimRecord value);
}
