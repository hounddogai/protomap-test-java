package com.acme.finance.domains.compliance;

import com.acme.compliance.v1.Sanction;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class SanctionRecordMapperImpl implements SanctionRecordMapper {
    @Override
    public SanctionRecord toRecord(Sanction value) {
        return new SanctionRecord(value.getSanctionId(), value.getName(), value.toByteArray());
    }

    @Override
    public Sanction toProto(SanctionRecord value) {
        try {
            return Sanction.parseFrom(value.snapshot()).toBuilder()
                    .setSanctionId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
