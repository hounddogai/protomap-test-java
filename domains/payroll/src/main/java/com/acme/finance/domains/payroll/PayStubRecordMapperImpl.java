package com.acme.finance.domains.payroll;

import com.acme.payroll.v1.PayStub;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class PayStubRecordMapperImpl implements PayStubRecordMapper {
    @Override
    public PayStubRecord toRecord(PayStub value) {
        return new PayStubRecord(value.getPayStubId(), value.getNumber(), value.toByteArray());
    }

    @Override
    public PayStub toProto(PayStubRecord value) {
        try {
            return PayStub.parseFrom(value.snapshot()).toBuilder()
                    .setPayStubId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
