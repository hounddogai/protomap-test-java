package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.Accrual;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class AccrualRecordMapperImpl implements AccrualRecordMapper {
    @Override
    public AccrualRecord toRecord(Accrual value) {
        return new AccrualRecord(value.getAccrualId(), value.getNumber(), value.toByteArray());
    }

    @Override
    public Accrual toProto(AccrualRecord value) {
        try {
            return Accrual.parseFrom(value.snapshot()).toBuilder()
                    .setAccrualId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
