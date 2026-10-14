package com.acme.finance.domains.invoicing;

import com.acme.invoicing.v1.InvoiceTemplate;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface InvoiceTemplateRecordMapper {
    InvoiceTemplateRecord toRecord(InvoiceTemplate value);

    InvoiceTemplate toProto(InvoiceTemplateRecord value);
}
