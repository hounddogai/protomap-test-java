package com.acme.finance.domains.lending;

import com.acme.lending.v1.Collateral;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class CollateralRecordMapperImpl implements CollateralRecordMapper {
    @Override
    public CollateralRecord toRecord(Collateral value) {
        return new CollateralRecord(value.getCollateralId(), value.getCode(), value.toByteArray());
    }

    @Override
    public Collateral toProto(CollateralRecord value) {
        try {
            return Collateral.parseFrom(value.snapshot()).toBuilder()
                    .setCollateralId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
