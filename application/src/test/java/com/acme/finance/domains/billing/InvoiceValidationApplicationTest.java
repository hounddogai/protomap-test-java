package com.acme.finance.domains.billing;

import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.assets.v1.SearchFixedAssetsRequest;
import com.acme.assets.v1.SearchFixedAssetsResponse;
import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.BrowseAuditEventsRequest;
import com.acme.audit.v1.BrowseAuditEventsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.BillingAccountCardFunding;
import com.acme.billing.v1.BillingAccountValidationServiceGrpc;
import com.acme.billing.v1.CheckInvoiceEligibilityRequest;
import com.acme.billing.v1.CheckInvoiceEligibilityResponse;
import com.acme.billing.v1.CompareCreditNoteOptionsRequest;
import com.acme.billing.v1.CompareCreditNoteOptionsResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.CreditNoteAttachment;
import com.acme.billing.v1.CreditNoteCalculationServiceGrpc;
import com.acme.billing.v1.CreditNoteCardSettlement;
import com.acme.billing.v1.Dunning;
import com.acme.billing.v1.ExplainInvoiceDecisionRequest;
import com.acme.billing.v1.ExplainInvoiceDecisionResponse;
import com.acme.billing.v1.GetInvoiceRulesRequest;
import com.acme.billing.v1.GetInvoiceRulesResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceRef;
import com.acme.billing.v1.LintBillingAccountsRequest;
import com.acme.billing.v1.LintBillingAccountsResponse;
import com.acme.billing.v1.LintInvoicesRequest;
import com.acme.billing.v1.LintInvoicesResponse;
import com.acme.billing.v1.NormalizeInvoiceRequest;
import com.acme.billing.v1.NormalizeInvoiceResponse;
import com.acme.billing.v1.PaymentTerm;
import com.acme.billing.v1.PreviewInvoiceRequest;
import com.acme.billing.v1.PreviewInvoiceResponse;
import com.acme.billing.v1.QuoteCreditNoteRequest;
import com.acme.billing.v1.QuoteCreditNoteResponse;
import com.acme.billing.v1.RecalculateCreditNotesRequest;
import com.acme.billing.v1.RecalculateCreditNotesResponse;
import com.acme.billing.v1.ScoreBillingAccountRequest;
import com.acme.billing.v1.ScoreBillingAccountResponse;
import com.acme.billing.v1.ScoreInvoiceRequest;
import com.acme.billing.v1.ScoreInvoiceResponse;
import com.acme.billing.v1.SimulateCreditNoteRequest;
import com.acme.billing.v1.SimulateCreditNoteResponse;
import com.acme.billing.v1.ValidateInvoiceRequest;
import com.acme.billing.v1.ValidateInvoiceResponse;
import com.acme.collections.v1.BatchCreateCollectionCasesRequest;
import com.acme.collections.v1.BatchCreateCollectionCasesResponse;
import com.acme.collections.v1.CheckWriteOffEligibilityRequest;
import com.acme.collections.v1.CheckWriteOffEligibilityResponse;
import com.acme.collections.v1.CollectionCaseServiceGrpc;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffLine;
import com.acme.collections.v1.WriteOffLineTax;
import com.acme.collections.v1.WriteOffValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.expenses.v1.CheckExpensePolicyEligibilityRequest;
import com.acme.expenses.v1.CheckExpensePolicyEligibilityResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicyValidationServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.invoicing.v1.FindInvoiceBatchesRequest;
import com.acme.invoicing.v1.FindInvoiceBatchesResponse;
import com.acme.invoicing.v1.InvoiceBatchQueryServiceGrpc;
import com.acme.ledger.v1.CreateLedgerAccountRequest;
import com.acme.ledger.v1.CreateLedgerAccountResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountServiceGrpc;
import com.acme.metrics.v1.MetricSeriesQueryServiceGrpc;
import com.acme.metrics.v1.SuggestMetricSeriesesRequest;
import com.acme.metrics.v1.SuggestMetricSeriesesResponse;
import com.acme.metrics.v1.SummarizeMetricSeriesesRequest;
import com.acme.metrics.v1.SummarizeMetricSeriesesResponse;
import com.acme.notifications.v1.FindNotificationsRequest;
import com.acme.notifications.v1.FindNotificationsResponse;
import com.acme.notifications.v1.NotificationQueryServiceGrpc;
import com.acme.notifications.v1.ResolveNotificationsRequest;
import com.acme.notifications.v1.ResolveNotificationsResponse;
import com.acme.payroll.v1.EstimatePayStubRequest;
import com.acme.payroll.v1.EstimatePayStubResponse;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubCalculationServiceGrpc;
import com.acme.recommendations.v1.BatchGetRecommendationSetsRequest;
import com.acme.recommendations.v1.BatchGetRecommendationSetsResponse;
import com.acme.recommendations.v1.RecommendationSetServiceGrpc;
import com.acme.search.v1.SearchIndexServiceGrpc;
import com.acme.search.v1.SearchSearchIndexesRequest;
import com.acme.search.v1.SearchSearchIndexesResponse;
import com.acme.treasury.v1.BankTransferQueryServiceGrpc;
import com.acme.treasury.v1.BrowseBankTransfersRequest;
import com.acme.treasury.v1.BrowseBankTransfersResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class InvoiceValidationApplicationTest {
    @Test
    void validateInvoicePreservesPlannedFields() {
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
        var listener = new InvoiceValidationApplicationEvents(PayStubCalculationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof InvoiceValidationApplication.ScoreInvoiceStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new InvoiceValidationApplication(
                FixedAssetServiceGrpc.newBlockingStub(channel),
                AuditEventQueryServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                BillingAccountValidationServiceGrpc.newBlockingStub(channel),
                CreditNoteCalculationServiceGrpc.newBlockingStub(channel),
                CreditNoteCalculationServiceGrpc.newFutureStub(channel),
                CollectionCaseServiceGrpc.newStub(channel),
                WriteOffValidationServiceGrpc.newBlockingStub(channel),
                events,
                ExpensePolicyValidationServiceGrpc.newBlockingStub(channel),
                InvoiceBatchQueryServiceGrpc.newBlockingStub(channel),
                LedgerAccountServiceGrpc.newBlockingStub(channel),
                MetricSeriesQueryServiceGrpc.newBlockingStub(channel),
                NotificationQueryServiceGrpc.newBlockingStub(channel),
                PayStubCalculationServiceGrpc.newBlockingStub(channel),
                RecommendationSetServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SearchIndexServiceGrpc.newBlockingStub(channel),
                BankTransferQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ValidateInvoiceRequest.getDefaultInstance());
        var response = application.validateInvoice(request);
        var sent0 = channel.request("acme.invoicing.v1.InvoiceBatchQueryService/FindInvoiceBatches");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice", "billing_address", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice", "issued_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        var sent1 = channel.request("acme.payroll.v1.PayStubCalculationService/EstimatePayStub");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice", "billing_address", "locality"),
                ProbeSupport.value(sent1, "details", "destination", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice", "billing_address", "country_code"),
                ProbeSupport.value(sent1, "pay_stub", "billing_address", "country_code"));
        var sent2 = channel.request("acme.expenses.v1.ExpensePolicyValidationService/CheckExpensePolicyEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayStubCalculationService/EstimatePayStub"), "pay_stub", "audit", "created_by"),
                ProbeSupport.value(sent2, "expense_policy", "audit", "created_by"));
        var sent3 = channel.request("acme.audit.v1.AuditEventQueryService/BrowseAuditEvents");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayStubCalculationService/EstimatePayStub"), "pay_stub", "billing_address", "region"),
                ProbeSupport.value(sent3, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice", "audit", "updated_at"),
                ProbeSupport.value(sent3, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice", "counterparty", "customer_id"),
                ProbeSupport.value(sent3, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceBatchQueryService/FindInvoiceBatches"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceBatchQueryService/FindInvoiceBatches"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayStubCalculationService/EstimatePayStub"), "pay_stub", "billing_address", "lines"),
                ProbeSupport.value(response, "invoice", "billing_address", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpensePolicyValidationService/CheckExpensePolicyEligibility"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_id"),
                ProbeSupport.value(response, "invoice", "invoice_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice", "notes"),
                ProbeSupport.value(response, "invoice", "notes"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new InvoiceValidationApplicationEvents(PayStubCalculationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<EstimatePayStubResponse>();
        listener.on(new InvoiceValidationApplication.ScoreInvoiceStage1Requested(ProbeSupport.sample(EstimatePayStubRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
