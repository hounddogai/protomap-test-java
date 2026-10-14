package com.acme.finance.domains.expenses;

import com.acme.expenses.v1.Receipt;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class ReceiptRecordMapperImpl implements ReceiptRecordMapper {
    @Override
    public ReceiptRecord toRecord(Receipt value) {
        return new ReceiptRecord(value.getReceiptId(), value.getNumber(), value.toByteArray());
    }

    @Override
    public Receipt toProto(ReceiptRecord value) {
        try {
            return Receipt.parseFrom(value.snapshot()).toBuilder()
                    .setReceiptId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
