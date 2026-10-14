package com.acme.finance.domains.financeportal;

import com.acme.financeportal.v1.CloseChecklist;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class CloseChecklistRecordMapperImpl implements CloseChecklistRecordMapper {
    @Override
    public CloseChecklistRecord toRecord(CloseChecklist value) {
        return new CloseChecklistRecord(value.getCloseChecklistId(), value.getNumber(), value.toByteArray());
    }

    @Override
    public CloseChecklist toProto(CloseChecklistRecord value) {
        try {
            return CloseChecklist.parseFrom(value.snapshot()).toBuilder()
                    .setCloseChecklistId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
