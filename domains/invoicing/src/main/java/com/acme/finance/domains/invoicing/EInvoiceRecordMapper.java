package com.acme.finance.domains.invoicing;

import com.acme.invoicing.v1.EInvoice;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface EInvoiceRecordMapper {
    EInvoiceRecord toRecord(EInvoice value);

    EInvoice toProto(EInvoiceRecord value);
}
