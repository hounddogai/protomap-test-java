package com.acme.finance.domains.billing;

import com.acme.billing.v1.Dunning;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class DunningRecordMapperImpl implements DunningRecordMapper {
    @Override
    public DunningRecord toRecord(Dunning value) {
        return new DunningRecord(value.getDunningId(), value.getSubject(), value.toByteArray());
    }

    @Override
    public Dunning toProto(DunningRecord value) {
        try {
            return Dunning.parseFrom(value.snapshot()).toBuilder()
                    .setDunningId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
