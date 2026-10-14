package com.acme.finance.domains.billing;

import com.acme.billing.v1.CreditNote;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class CreditNoteRecordMapperImpl implements CreditNoteRecordMapper {
    @Override
    public CreditNoteRecord toRecord(CreditNote value) {
        return new CreditNoteRecord(value.getCreditNoteId(), value.getNumber(), value.toByteArray());
    }

    @Override
    public CreditNote toProto(CreditNoteRecord value) {
        try {
            return CreditNote.parseFrom(value.snapshot()).toBuilder()
                    .setCreditNoteId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
