package com.acme.finance.domains.assets;

import com.acme.assets.v1.Depreciation;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface DepreciationRecordMapper {
    DepreciationRecord toRecord(Depreciation value);

    Depreciation toProto(DepreciationRecord value);
}
