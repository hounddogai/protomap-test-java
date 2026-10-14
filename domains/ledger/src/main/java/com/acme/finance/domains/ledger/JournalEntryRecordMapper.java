package com.acme.finance.domains.ledger;

import com.acme.ledger.v1.JournalEntry;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface JournalEntryRecordMapper {
    JournalEntryRecord toRecord(JournalEntry value);

    JournalEntry toProto(JournalEntryRecord value);
}
