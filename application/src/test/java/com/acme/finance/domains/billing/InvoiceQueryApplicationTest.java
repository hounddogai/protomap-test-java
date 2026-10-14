package com.acme.finance.domains.billing;

import com.acme.admissions.v1.Admission;
import com.acme.admissions.v1.AdmissionCalculationServiceGrpc;
import com.acme.admissions.v1.AdmissionMeasurement;
import com.acme.admissions.v1.RecalculateAdmissionsRequest;
import com.acme.admissions.v1.RecalculateAdmissionsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.SubscribeRetentionPolicyUpdatesRequest;
import com.acme.audit.v1.SubscribeRetentionPolicyUpdatesResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.BillingAccountQueryServiceGrpc;
import com.acme.billing.v1.BrowseInvoicesRequest;
import com.acme.billing.v1.BrowseInvoicesResponse;
import com.acme.billing.v1.CountInvoicesRequest;
import com.acme.billing.v1.CountInvoicesResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.CreditNoteCalculationServiceGrpc;
import com.acme.billing.v1.CreditNoteLine;
import com.acme.billing.v1.Dunning;
import com.acme.billing.v1.FindBillingAccountsRequest;
import com.acme.billing.v1.FindBillingAccountsResponse;
import com.acme.billing.v1.FindInvoicesRequest;
import com.acme.billing.v1.FindInvoicesResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceLine;
import com.acme.billing.v1.InvoiceRef;
import com.acme.billing.v1.LintPaymentTermsRequest;
import com.acme.billing.v1.LintPaymentTermsResponse;
import com.acme.billing.v1.LookupInvoiceRequest;
import com.acme.billing.v1.LookupInvoiceResponse;
import com.acme.billing.v1.PaymentTerm;
import com.acme.billing.v1.PaymentTermValidationServiceGrpc;
import com.acme.billing.v1.PeekInvoiceRequest;
import com.acme.billing.v1.PeekInvoiceResponse;
import com.acme.billing.v1.QuoteCreditNoteRequest;
import com.acme.billing.v1.QuoteCreditNoteResponse;
import com.acme.billing.v1.ResolveInvoicesRequest;
import com.acme.billing.v1.ResolveInvoicesResponse;
import com.acme.billing.v1.SuggestInvoicesRequest;
import com.acme.billing.v1.SuggestInvoicesResponse;
import com.acme.billing.v1.SummarizeInvoicesRequest;
import com.acme.billing.v1.SummarizeInvoicesResponse;
import com.acme.budgeting.v1.BatchCreateBudgetsRequest;
import com.acme.budgeting.v1.BatchCreateBudgetsResponse;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineValidationServiceGrpc;
import com.acme.budgeting.v1.BudgetServiceGrpc;
import com.acme.budgeting.v1.NormalizeBudgetLineRequest;
import com.acme.budgeting.v1.NormalizeBudgetLineResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.procurement.v1.PurchaseRequisitionServiceGrpc;
import com.acme.procurement.v1.WatchPurchaseRequisitionsRequest;
import com.acme.procurement.v1.WatchPurchaseRequisitionsResponse;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionValidationServiceGrpc;
import com.acme.treasury.v1.CheckCashPositionEligibilityRequest;
import com.acme.treasury.v1.CheckCashPositionEligibilityResponse;
import com.acme.treasury.v1.ExplainCashPositionDecisionRequest;
import com.acme.treasury.v1.ExplainCashPositionDecisionResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class InvoiceQueryApplicationTest {
    @Test
    void summarizeInvoicesPreservesPlannedFields() {
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
        var listener = new InvoiceQueryApplicationEvents(BudgetLineValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof InvoiceQueryApplication.LookupInvoiceStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new InvoiceQueryApplication(
                AdmissionCalculationServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                CreditNoteCalculationServiceGrpc.newBlockingStub(channel),
                PaymentTermValidationServiceGrpc.newBlockingStub(channel),
                BudgetServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                events,
                PurchaseRequisitionServiceGrpc.newBlockingStub(channel),
                repository,
                CashPositionValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SummarizeInvoicesRequest.getDefaultInstance());
        var response = application.summarizeInvoices(request);
        var sent0 = channel.request("acme.billing.v1.CreditNoteCalculationService/QuoteCreditNote");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "annotations"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent0, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_id"),
                ProbeSupport.value(sent0, "credit_note", "counterparty", "invoice_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "filters"),
                ProbeSupport.value(sent0, "credit_note", "lines", "attributes"));
        var sent1 = channel.request("acme.billing.v1.BillingAccountQueryService/FindBillingAccounts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.CreditNoteCalculationService/QuoteCreditNote"), "credit_note", "billing_address", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        var sent2 = channel.request("acme.audit.v1.RetentionPolicyNotificationService/SubscribeRetentionPolicyUpdates");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent2, "reason"));
        var sent3 = channel.request("acme.billing.v1.PaymentTermValidationService/LintPaymentTerms");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.CreditNoteCalculationService/QuoteCreditNote"), "credit_note", "audit", "etag"),
                ProbeSupport.value(sent3, "payment_term", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.CreditNoteCalculationService/QuoteCreditNote"), "credit_note", "audit", "created_by"),
                ProbeSupport.value(sent3, "payment_term", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.CreditNoteCalculationService/QuoteCreditNote"), "credit_note", "audit", "updated_at"),
                ProbeSupport.value(sent3, "payment_term", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.CreditNoteCalculationService/QuoteCreditNote"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.CreditNoteCalculationService/QuoteCreditNote"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.BillingAccountQueryService/FindBillingAccounts"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyNotificationService/SubscribeRetentionPolicyUpdates"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "period"),
                ProbeSupport.value(response, "period"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new InvoiceQueryApplicationEvents(BudgetLineValidationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<NormalizeBudgetLineResponse>();
        listener.on(new InvoiceQueryApplication.LookupInvoiceStage0Requested(ProbeSupport.sample(NormalizeBudgetLineRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
