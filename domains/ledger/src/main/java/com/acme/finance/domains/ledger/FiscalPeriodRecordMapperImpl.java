package com.acme.finance.domains.ledger;

import com.acme.ledger.v1.FiscalPeriod;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class FiscalPeriodRecordMapperImpl implements FiscalPeriodRecordMapper {
    @Override
    public FiscalPeriodRecord toRecord(FiscalPeriod value) {
        return new FiscalPeriodRecord(value.getFiscalPeriodId(), value.getTitle(), value.toByteArray());
    }

    @Override
    public FiscalPeriod toProto(FiscalPeriodRecord value) {
        try {
            return FiscalPeriod.parseFrom(value.snapshot()).toBuilder()
                    .setFiscalPeriodId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
