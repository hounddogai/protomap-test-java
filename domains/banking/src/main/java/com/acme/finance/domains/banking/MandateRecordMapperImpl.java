package com.acme.finance.domains.banking;

import com.acme.banking.v1.Mandate;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class MandateRecordMapperImpl implements MandateRecordMapper {
    @Override
    public MandateRecord toRecord(Mandate value) {
        return new MandateRecord(value.getMandateId(), value.getNumber(), value.toByteArray());
    }

    @Override
    public Mandate toProto(MandateRecord value) {
        try {
            return Mandate.parseFrom(value.snapshot()).toBuilder()
                    .setMandateId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
