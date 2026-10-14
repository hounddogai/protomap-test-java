package com.acme.finance.domains.billing;

import com.acme.billing.v1.BillingAccount;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class BillingAccountRecordMapperImpl implements BillingAccountRecordMapper {
    @Override
    public BillingAccountRecord toRecord(BillingAccount value) {
        return new BillingAccountRecord(value.getBillingAccountId(), value.getCurrencyCode(), value.toByteArray());
    }

    @Override
    public BillingAccount toProto(BillingAccountRecord value) {
        try {
            return BillingAccount.parseFrom(value.snapshot()).toBuilder()
                    .setBillingAccountId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
