package com.acme.finance.domains.financeportal;

import com.acme.financeportal.v1.CloseChecklist;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface CloseChecklistRecordMapper {
    CloseChecklistRecord toRecord(CloseChecklist value);

    CloseChecklist toProto(CloseChecklistRecord value);
}
