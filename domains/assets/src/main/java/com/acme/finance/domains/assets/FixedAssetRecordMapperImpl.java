package com.acme.finance.domains.assets;

import com.acme.assets.v1.FixedAsset;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class FixedAssetRecordMapperImpl implements FixedAssetRecordMapper {
    @Override
    public FixedAssetRecord toRecord(FixedAsset value) {
        return new FixedAssetRecord(value.getFixedAssetId(), value.getCode(), value.toByteArray());
    }

    @Override
    public FixedAsset toProto(FixedAssetRecord value) {
        try {
            return FixedAsset.parseFrom(value.snapshot()).toBuilder()
                    .setFixedAssetId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
