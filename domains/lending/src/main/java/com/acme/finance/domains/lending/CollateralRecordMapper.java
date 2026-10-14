package com.acme.finance.domains.lending;

import com.acme.lending.v1.Collateral;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface CollateralRecordMapper {
    CollateralRecord toRecord(Collateral value);

    Collateral toProto(CollateralRecord value);
}
