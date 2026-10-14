package com.acme.finance.domains.billing;

import com.acme.billing.v1.CreditNote;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface CreditNoteRecordMapper {
    CreditNoteRecord toRecord(CreditNote value);

    CreditNote toProto(CreditNoteRecord value);
}
