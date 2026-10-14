package com.acme.finance.domains.contracts;

import com.acme.contracts.v1.Renewal;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class RenewalRecordMapperImpl implements RenewalRecordMapper {
    @Override
    public RenewalRecord toRecord(Renewal value) {
        return new RenewalRecord(value.getRenewalId(), value.getTitle(), value.toByteArray());
    }

    @Override
    public Renewal toProto(RenewalRecord value) {
        try {
            return Renewal.parseFrom(value.snapshot()).toBuilder()
                    .setRenewalId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
