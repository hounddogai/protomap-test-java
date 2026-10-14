package com.acme.finance.domains.billing;

import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.audit.v1.NormalizeAuditTrailRequest;
import com.acme.audit.v1.NormalizeAuditTrailResponse;
import com.acme.billing.v1.ActivatePaymentTermRequest;
import com.acme.billing.v1.ActivatePaymentTermResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.CalculateCreditNoteRequest;
import com.acme.billing.v1.CalculateCreditNoteResponse;
import com.acme.billing.v1.CalculatePaymentTermRequest;
import com.acme.billing.v1.CalculatePaymentTermResponse;
import com.acme.billing.v1.CompareCreditNoteOptionsRequest;
import com.acme.billing.v1.CompareCreditNoteOptionsResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.CreditNoteCalculationServiceGrpc;
import com.acme.billing.v1.CreditNoteValidationServiceGrpc;
import com.acme.billing.v1.Dunning;
import com.acme.billing.v1.DunningReportingServiceGrpc;
import com.acme.billing.v1.ExpirePaymentTermRequest;
import com.acme.billing.v1.ExpirePaymentTermResponse;
import com.acme.billing.v1.GenerateDunningReportRequest;
import com.acme.billing.v1.GenerateDunningReportResponse;
import com.acme.billing.v1.GetPaymentTermLifecycleRequest;
import com.acme.billing.v1.GetPaymentTermLifecycleResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceLine;
import com.acme.billing.v1.InvoiceLineTax;
import com.acme.billing.v1.InvoiceQueryServiceGrpc;
import com.acme.billing.v1.InvoiceRef;
import com.acme.billing.v1.InvoiceValidationServiceGrpc;
import com.acme.billing.v1.NormalizeCreditNoteRequest;
import com.acme.billing.v1.NormalizeCreditNoteResponse;
import com.acme.billing.v1.NormalizeInvoiceRequest;
import com.acme.billing.v1.NormalizeInvoiceResponse;
import com.acme.billing.v1.PaymentTerm;
import com.acme.billing.v1.PaymentTermCalculationServiceGrpc;
import com.acme.billing.v1.ReinstatePaymentTermRequest;
import com.acme.billing.v1.ReinstatePaymentTermResponse;
import com.acme.billing.v1.RenewPaymentTermRequest;
import com.acme.billing.v1.RenewPaymentTermResponse;
import com.acme.billing.v1.ResolveInvoicesRequest;
import com.acme.billing.v1.ResolveInvoicesResponse;
import com.acme.billing.v1.SchedulePaymentTermExpiryRequest;
import com.acme.billing.v1.SchedulePaymentTermExpiryResponse;
import com.acme.billing.v1.ScoreInvoiceRequest;
import com.acme.billing.v1.ScoreInvoiceResponse;
import com.acme.billing.v1.ShareDunningReportRequest;
import com.acme.billing.v1.ShareDunningReportResponse;
import com.acme.billing.v1.SuspendPaymentTermRequest;
import com.acme.billing.v1.SuspendPaymentTermResponse;
import com.acme.billing.v1.TerminatePaymentTermRequest;
import com.acme.billing.v1.TerminatePaymentTermResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.ContractClause;
import com.acme.contracts.v1.ContractClauseApprovalServiceGrpc;
import com.acme.contracts.v1.DelegateContractClauseApprovalRequest;
import com.acme.contracts.v1.DelegateContractClauseApprovalResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.expenses.v1.MileageClaimRef;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptApprovalServiceGrpc;
import com.acme.expenses.v1.SubmitReceiptRequest;
import com.acme.expenses.v1.SubmitReceiptResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesValidationServiceGrpc;
import com.acme.metrics.v1.PreviewMetricSeriesRequest;
import com.acme.metrics.v1.PreviewMetricSeriesResponse;
import com.acme.onboarding.v1.ListOnboardingPlansRequest;
import com.acme.onboarding.v1.ListOnboardingPlansResponse;
import com.acme.onboarding.v1.OnboardingPlanServiceGrpc;
import com.google.protobuf.Any;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class PaymentTermLifecycleApplicationTest {
    @Test
    void schedulePaymentTermExpiryPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BillingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBillingAccount(String tenant, BillingAccount value) {}
            public void saveCreditNote(String tenant, CreditNote value) {}
            public void saveDunning(String tenant, Dunning value) {}
            public void saveInvoice(String tenant, Invoice value) {}
            public void savePaymentTerm(String tenant, PaymentTerm value) {}
        };
        var application = new PaymentTermLifecycleApplication(
                AuditTrailValidationServiceGrpc.newBlockingStub(channel),
                CreditNoteCalculationServiceGrpc.newBlockingStub(channel),
                CreditNoteValidationServiceGrpc.newBlockingStub(channel),
                DunningReportingServiceGrpc.newBlockingStub(channel),
                InvoiceQueryServiceGrpc.newBlockingStub(channel),
                InvoiceValidationServiceGrpc.newBlockingStub(channel),
                PaymentTermCalculationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ContractClauseApprovalServiceGrpc.newBlockingStub(channel),
                ReceiptApprovalServiceGrpc.newBlockingStub(channel),
                MetricSeriesValidationServiceGrpc.newBlockingStub(channel),
                OnboardingPlanServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(SchedulePaymentTermExpiryRequest.getDefaultInstance());
        var response = application.schedulePaymentTermExpiry(request);
        var sent0 = channel.request("acme.billing.v1.InvoiceValidationService/ScoreInvoice");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "payment_term", "discount", "amount"),
                ProbeSupport.value(sent0, "invoice", "lines", "tax", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "payment_term", "audit", "updated_at"),
                ProbeSupport.value(sent0, "invoice", "audit", "updated_at"));
        var sent1 = channel.request("acme.billing.v1.PaymentTermCalculationService/CalculatePaymentTerm");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceValidationService/ScoreInvoice"), "invoice", "audit", "updated_at"),
                ProbeSupport.value(sent1, "payment_term", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "payment_term", "owner", "user_id"),
                ProbeSupport.value(sent1, "payment_term", "owner", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceValidationService/ScoreInvoice"), "invoice", "billing_address", "country_code"),
                ProbeSupport.value(sent1, "details", "destination", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceValidationService/ScoreInvoice"), "invoice", "billing_address", "locality"),
                ProbeSupport.value(sent1, "details", "destination", "locality"));
        var sent2 = channel.request("acme.billing.v1.CreditNoteValidationService/NormalizeCreditNote");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceValidationService/ScoreInvoice"), "invoice", "audit", "created_by"),
                ProbeSupport.value(sent2, "credit_note", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceValidationService/ScoreInvoice"), "invoice", "audit", "updated_by"),
                ProbeSupport.value(sent2, "credit_note", "audit", "updated_by"));
        var sent3 = channel.request("acme.billing.v1.DunningReportingService/GenerateDunningReport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.PaymentTermCalculationService/CalculatePaymentTerm"), "payment_term", "block", "reason"),
                ProbeSupport.value(sent3, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent3, "annotations"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceValidationService/ScoreInvoice"), "invoice", "counterparty", "display_name"),
                ProbeSupport.value(response, "payment_term", "owner", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceValidationService/ScoreInvoice"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.PaymentTermCalculationService/CalculatePaymentTerm"), "payment_term", "effective"),
                ProbeSupport.value(response, "payment_term", "effective"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.CreditNoteValidationService/NormalizeCreditNote"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.DunningReportingService/GenerateDunningReport"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.DunningReportingService/GenerateDunningReport"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "payment_term_id"),
                ProbeSupport.value(response, "payment_term", "payment_term_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "payment_term", "version"),
                ProbeSupport.value(response, "payment_term", "version"));
    }
}
