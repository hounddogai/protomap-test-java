package com.acme.finance.domains.invoicing;

import com.acme.invoicing.v1.EInvoice;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class EInvoiceRecordMapperImpl implements EInvoiceRecordMapper {
    @Override
    public EInvoiceRecord toRecord(EInvoice value) {
        return new EInvoiceRecord(value.getEInvoiceId(), value.getNumber(), value.toByteArray());
    }

    @Override
    public EInvoice toProto(EInvoiceRecord value) {
        try {
            return EInvoice.parseFrom(value.snapshot()).toBuilder()
                    .setEInvoiceId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
