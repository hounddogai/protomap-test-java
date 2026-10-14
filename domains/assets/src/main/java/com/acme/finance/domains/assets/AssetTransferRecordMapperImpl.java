package com.acme.finance.domains.assets;

import com.acme.assets.v1.AssetTransfer;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class AssetTransferRecordMapperImpl implements AssetTransferRecordMapper {
    @Override
    public AssetTransferRecord toRecord(AssetTransfer value) {
        return new AssetTransferRecord(value.getAssetTransferId(), value.getNumber(), value.toByteArray());
    }

    @Override
    public AssetTransfer toProto(AssetTransferRecord value) {
        try {
            return AssetTransfer.parseFrom(value.snapshot()).toBuilder()
                    .setAssetTransferId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
