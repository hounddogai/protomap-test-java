package com.acme.finance.domains.invoicing;

import com.acme.invoicing.v1.InvoiceBatch;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class InvoiceBatchRecordMapperImpl implements InvoiceBatchRecordMapper {
    @Override
    public InvoiceBatchRecord toRecord(InvoiceBatch value) {
        return new InvoiceBatchRecord(value.getInvoiceBatchId(), value.getNumber(), value.toByteArray());
    }

    @Override
    public InvoiceBatch toProto(InvoiceBatchRecord value) {
        try {
            return InvoiceBatch.parseFrom(value.snapshot()).toBuilder()
                    .setInvoiceBatchId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
