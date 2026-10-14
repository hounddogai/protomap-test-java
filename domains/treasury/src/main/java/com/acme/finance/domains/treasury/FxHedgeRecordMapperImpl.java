package com.acme.finance.domains.treasury;

import com.acme.treasury.v1.FxHedge;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class FxHedgeRecordMapperImpl implements FxHedgeRecordMapper {
    @Override
    public FxHedgeRecord toRecord(FxHedge value) {
        return new FxHedgeRecord(value.getFxHedgeId(), value.getNumber(), value.toByteArray());
    }

    @Override
    public FxHedge toProto(FxHedgeRecord value) {
        try {
            return FxHedge.parseFrom(value.snapshot()).toBuilder()
                    .setFxHedgeId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
