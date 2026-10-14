package com.acme.finance.domains.contracts;

import com.acme.contracts.v1.Obligation;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class ObligationRecordMapperImpl implements ObligationRecordMapper {
    @Override
    public ObligationRecord toRecord(Obligation value) {
        return new ObligationRecord(value.getObligationId(), value.getSubject(), value.toByteArray());
    }

    @Override
    public Obligation toProto(ObligationRecord value) {
        try {
            return Obligation.parseFrom(value.snapshot()).toBuilder()
                    .setObligationId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
