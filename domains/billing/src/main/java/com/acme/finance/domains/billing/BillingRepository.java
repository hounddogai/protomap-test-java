package com.acme.finance.domains.billing;

import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.Dunning;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.PaymentTerm;

/**
 * The domain's durable record and audit port.
 */
public interface BillingRepository {
    void recordAction(String tenant, String kind, String id, String action);

    void recordDetail(String tenant, String operation, String detail);

    void saveBillingAccount(String tenant, BillingAccount value);

    void saveCreditNote(String tenant, CreditNote value);

    void saveDunning(String tenant, Dunning value);

    void saveInvoice(String tenant, Invoice value);

    void savePaymentTerm(String tenant, PaymentTerm value);
}
