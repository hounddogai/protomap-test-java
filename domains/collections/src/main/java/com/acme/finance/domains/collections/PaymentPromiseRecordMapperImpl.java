package com.acme.finance.domains.collections;

import com.acme.collections.v1.PaymentPromise;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class PaymentPromiseRecordMapperImpl implements PaymentPromiseRecordMapper {
    @Override
    public PaymentPromiseRecord toRecord(PaymentPromise value) {
        return new PaymentPromiseRecord(value.getPaymentPromiseId(), value.getTitle(), value.toByteArray());
    }

    @Override
    public PaymentPromise toProto(PaymentPromiseRecord value) {
        try {
            return PaymentPromise.parseFrom(value.snapshot()).toBuilder()
                    .setPaymentPromiseId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
