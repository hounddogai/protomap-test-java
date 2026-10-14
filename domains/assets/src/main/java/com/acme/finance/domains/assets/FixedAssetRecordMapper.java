package com.acme.finance.domains.assets;

import com.acme.assets.v1.FixedAsset;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface FixedAssetRecordMapper {
    FixedAssetRecord toRecord(FixedAsset value);

    FixedAsset toProto(FixedAssetRecord value);
}
