package com.acme.finance.domains.invoicing;

import com.acme.invoicing.v1.InvoiceTemplate;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class InvoiceTemplateRecordMapperImpl implements InvoiceTemplateRecordMapper {
    @Override
    public InvoiceTemplateRecord toRecord(InvoiceTemplate value) {
        return new InvoiceTemplateRecord(value.getInvoiceTemplateId(), value.getNumber(), value.toByteArray());
    }

    @Override
    public InvoiceTemplate toProto(InvoiceTemplateRecord value) {
        try {
            return InvoiceTemplate.parseFrom(value.snapshot()).toBuilder()
                    .setInvoiceTemplateId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
