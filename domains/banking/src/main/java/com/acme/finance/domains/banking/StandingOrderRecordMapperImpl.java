package com.acme.finance.domains.banking;

import com.acme.banking.v1.StandingOrder;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class StandingOrderRecordMapperImpl implements StandingOrderRecordMapper {
    @Override
    public StandingOrderRecord toRecord(StandingOrder value) {
        return new StandingOrderRecord(value.getStandingOrderId(), value.getTitle(), value.toByteArray());
    }

    @Override
    public StandingOrder toProto(StandingOrderRecord value) {
        try {
            return StandingOrder.parseFrom(value.snapshot()).toBuilder()
                    .setStandingOrderId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
