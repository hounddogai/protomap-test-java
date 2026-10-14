package com.acme.finance.domains.insurance;

import com.acme.insurance.v1.Claim;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface ClaimRecordMapper {
    ClaimRecord toRecord(Claim value);

    Claim toProto(ClaimRecord value);
}
