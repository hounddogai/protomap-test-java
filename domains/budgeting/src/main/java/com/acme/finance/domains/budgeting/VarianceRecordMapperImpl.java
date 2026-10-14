package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.Variance;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class VarianceRecordMapperImpl implements VarianceRecordMapper {
    @Override
    public VarianceRecord toRecord(Variance value) {
        return new VarianceRecord(value.getVarianceId(), value.getSubject(), value.toByteArray());
    }

    @Override
    public Variance toProto(VarianceRecord value) {
        try {
            return Variance.parseFrom(value.snapshot()).toBuilder()
                    .setVarianceId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
