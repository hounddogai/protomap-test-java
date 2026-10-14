package com.acme.finance.domains.expenses;

import com.acme.expenses.v1.MileageClaim;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class MileageClaimRecordMapperImpl implements MileageClaimRecordMapper {
    @Override
    public MileageClaimRecord toRecord(MileageClaim value) {
        return new MileageClaimRecord(value.getMileageClaimId(), value.getNumber(), value.toByteArray());
    }

    @Override
    public MileageClaim toProto(MileageClaimRecord value) {
        try {
            return MileageClaim.parseFrom(value.snapshot()).toBuilder()
                    .setMileageClaimId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
