package com.acme.finance.domains.invoicing;

import com.acme.invoicing.v1.EInvoice;
import com.acme.invoicing.v1.InvoiceBatch;
import com.acme.invoicing.v1.InvoiceTemplate;

/**
 * The domain's durable record and audit port.
 */
public interface InvoicingRepository {
    void recordAction(String tenant, String kind, String id, String action);

    void recordDetail(String tenant, String operation, String detail);

    void saveEInvoice(String tenant, EInvoice value);

    void saveInvoiceBatch(String tenant, InvoiceBatch value);

    void saveInvoiceTemplate(String tenant, InvoiceTemplate value);
}
