package com.acme.finance.domains.ledger;

import com.acme.ledger.v1.JournalEntry;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class JournalEntryRecordMapperImpl implements JournalEntryRecordMapper {
    @Override
    public JournalEntryRecord toRecord(JournalEntry value) {
        return new JournalEntryRecord(value.getJournalEntryId(), value.getNumber(), value.toByteArray());
    }

    @Override
    public JournalEntry toProto(JournalEntryRecord value) {
        try {
            return JournalEntry.parseFrom(value.snapshot()).toBuilder()
                    .setJournalEntryId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
