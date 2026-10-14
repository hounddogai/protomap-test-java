package com.acme.finance.domains.assets;

import com.acme.assets.v1.AssetTransfer;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface AssetTransferRecordMapper {
    AssetTransferRecord toRecord(AssetTransfer value);

    AssetTransfer toProto(AssetTransferRecord value);
}
