package com.acme.finance.domains.treasury;

import com.acme.treasury.v1.BankTransfer;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class BankTransferRecordMapperImpl implements BankTransferRecordMapper {
    @Override
    public BankTransferRecord toRecord(BankTransfer value) {
        return new BankTransferRecord(value.getBankTransferId(), value.getNumber(), value.toByteArray());
    }

    @Override
    public BankTransfer toProto(BankTransferRecord value) {
        try {
            return BankTransfer.parseFrom(value.snapshot()).toBuilder()
                    .setBankTransferId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
