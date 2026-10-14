package com.acme.finance.domains.billing;

import com.acme.billing.v1.PaymentTerm;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class PaymentTermRecordMapperImpl implements PaymentTermRecordMapper {
    @Override
    public PaymentTermRecord toRecord(PaymentTerm value) {
        return new PaymentTermRecord(value.getPaymentTermId(), value.getName(), value.toByteArray());
    }

    @Override
    public PaymentTerm toProto(PaymentTermRecord value) {
        try {
            return PaymentTerm.parseFrom(value.snapshot()).toBuilder()
                    .setPaymentTermId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
