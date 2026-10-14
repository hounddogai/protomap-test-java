package com.acme.finance.domains.billing;

import com.acme.billing.v1.Invoice;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface InvoiceRecordMapper {
    InvoiceRecord toRecord(Invoice value);

    Invoice toProto(InvoiceRecord value);
}
