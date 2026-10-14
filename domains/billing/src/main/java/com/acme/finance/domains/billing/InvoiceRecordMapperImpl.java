package com.acme.finance.domains.billing;

import com.acme.billing.v1.Invoice;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class InvoiceRecordMapperImpl implements InvoiceRecordMapper {
    @Override
    public InvoiceRecord toRecord(Invoice value) {
        return new InvoiceRecord(value.getInvoiceId(), value.getNumber(), value.toByteArray());
    }

    @Override
    public Invoice toProto(InvoiceRecord value) {
        try {
            return Invoice.parseFrom(value.snapshot()).toBuilder()
                    .setInvoiceId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
