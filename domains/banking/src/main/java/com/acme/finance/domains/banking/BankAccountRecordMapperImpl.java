package com.acme.finance.domains.banking;

import com.acme.banking.v1.BankAccount;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class BankAccountRecordMapperImpl implements BankAccountRecordMapper {
    @Override
    public BankAccountRecord toRecord(BankAccount value) {
        return new BankAccountRecord(value.getBankAccountId(), value.getCurrencyCode(), value.toByteArray());
    }

    @Override
    public BankAccount toProto(BankAccountRecord value) {
        try {
            return BankAccount.parseFrom(value.snapshot()).toBuilder()
                    .setBankAccountId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
