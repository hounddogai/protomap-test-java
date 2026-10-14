package com.acme.finance.domains.treasury;

import com.acme.treasury.v1.CashPosition;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class CashPositionRecordMapperImpl implements CashPositionRecordMapper {
    @Override
    public CashPositionRecord toRecord(CashPosition value) {
        return new CashPositionRecord(value.getCashPositionId(), value.getCurrencyCode(), value.toByteArray());
    }

    @Override
    public CashPosition toProto(CashPositionRecord value) {
        try {
            return CashPosition.parseFrom(value.snapshot()).toBuilder()
                    .setCashPositionId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
