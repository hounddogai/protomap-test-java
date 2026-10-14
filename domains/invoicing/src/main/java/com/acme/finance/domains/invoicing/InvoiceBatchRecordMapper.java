package com.acme.finance.domains.invoicing;

import com.acme.invoicing.v1.InvoiceBatch;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface InvoiceBatchRecordMapper {
    InvoiceBatchRecord toRecord(InvoiceBatch value);

    InvoiceBatch toProto(InvoiceBatchRecord value);
}
