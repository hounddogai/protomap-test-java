package com.acme.finance.domains.assets;

import com.acme.assets.v1.Depreciation;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class DepreciationRecordMapperImpl implements DepreciationRecordMapper {
    @Override
    public DepreciationRecord toRecord(Depreciation value) {
        return new DepreciationRecord(value.getDepreciationId(), value.getTitle(), value.toByteArray());
    }

    @Override
    public Depreciation toProto(DepreciationRecord value) {
        try {
            return Depreciation.parseFrom(value.snapshot()).toBuilder()
                    .setDepreciationId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
