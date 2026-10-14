package com.acme.finance.domains.insurance;

import com.acme.insurance.v1.Underwriting;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class UnderwritingRecordMapperImpl implements UnderwritingRecordMapper {
    @Override
    public UnderwritingRecord toRecord(Underwriting value) {
        return new UnderwritingRecord(value.getUnderwritingId(), value.getSubject(), value.toByteArray());
    }

    @Override
    public Underwriting toProto(UnderwritingRecord value) {
        try {
            return Underwriting.parseFrom(value.snapshot()).toBuilder()
                    .setUnderwritingId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
