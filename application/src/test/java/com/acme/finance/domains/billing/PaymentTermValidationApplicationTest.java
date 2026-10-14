package com.acme.finance.domains.billing;

import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.CheckPaymentTermEligibilityRequest;
import com.acme.billing.v1.CheckPaymentTermEligibilityResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.CreditNoteCalculationServiceGrpc;
import com.acme.billing.v1.CreditNoteLine;
import com.acme.billing.v1.CreditNoteLineTax;
import com.acme.billing.v1.CreditNoteValidationServiceGrpc;
import com.acme.billing.v1.Dunning;
import com.acme.billing.v1.ExplainCreditNoteCalculationRequest;
import com.acme.billing.v1.ExplainCreditNoteCalculationResponse;
import com.acme.billing.v1.ExplainCreditNoteDecisionRequest;
import com.acme.billing.v1.ExplainCreditNoteDecisionResponse;
import com.acme.billing.v1.ExplainPaymentTermCalculationRequest;
import com.acme.billing.v1.ExplainPaymentTermCalculationResponse;
import com.acme.billing.v1.ExplainPaymentTermDecisionRequest;
import com.acme.billing.v1.ExplainPaymentTermDecisionResponse;
import com.acme.billing.v1.GetPaymentTermRulesRequest;
import com.acme.billing.v1.GetPaymentTermRulesResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceCardSettlement;
import com.acme.billing.v1.InvoiceLine;
import com.acme.billing.v1.InvoiceLineTax;
import com.acme.billing.v1.InvoiceValidationServiceGrpc;
import com.acme.billing.v1.LintInvoicesRequest;
import com.acme.billing.v1.LintInvoicesResponse;
import com.acme.billing.v1.LintPaymentTermsRequest;
import com.acme.billing.v1.LintPaymentTermsResponse;
import com.acme.billing.v1.NormalizeCreditNoteRequest;
import com.acme.billing.v1.NormalizeCreditNoteResponse;
import com.acme.billing.v1.PaymentTerm;
import com.acme.billing.v1.PaymentTermCalculationServiceGrpc;
import com.acme.billing.v1.PaymentTermDiscountAction;
import com.acme.billing.v1.PreviewPaymentTermRequest;
import com.acme.billing.v1.PreviewPaymentTermResponse;
import com.acme.billing.v1.RecalculatePaymentTermsRequest;
import com.acme.billing.v1.RecalculatePaymentTermsResponse;
import com.acme.billing.v1.ScorePaymentTermRequest;
import com.acme.billing.v1.ScorePaymentTermResponse;
import com.acme.billing.v1.ValidateInvoiceRequest;
import com.acme.billing.v1.ValidateInvoiceResponse;
import com.acme.billing.v1.ValidatePaymentTermRequest;
import com.acme.billing.v1.ValidatePaymentTermResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetCalculationServiceGrpc;
import com.acme.budgeting.v1.CompareBudgetOptionsRequest;
import com.acme.budgeting.v1.CompareBudgetOptionsResponse;
import com.acme.budgeting.v1.ForecastRef;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagCondition;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.featureflags.v1.NormalizeFeatureFlagRequest;
import com.acme.featureflags.v1.NormalizeFeatureFlagResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.search.v1.PreviewSearchIndexRequest;
import com.acme.search.v1.PreviewSearchIndexResponse;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexValidationServiceGrpc;
import com.acme.travel.v1.SuggestTripsRequest;
import com.acme.travel.v1.SuggestTripsResponse;
import com.acme.travel.v1.TripQueryServiceGrpc;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferCalculationServiceGrpc;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionValidationServiceGrpc;
import com.acme.treasury.v1.PreviewCashPositionRequest;
import com.acme.treasury.v1.PreviewCashPositionResponse;
import com.acme.treasury.v1.SimulateBankTransferRequest;
import com.acme.treasury.v1.SimulateBankTransferResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class PaymentTermValidationApplicationTest {
    @Test
    void explainPaymentTermDecisionPreservesPlannedFields() {
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
        var listener = new PaymentTermValidationApplicationEvents(PaymentTermCalculationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof PaymentTermValidationApplication.ScorePaymentTermStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new PaymentTermValidationApplication(
                InvoiceValidationServiceGrpc.newBlockingStub(channel),
                PaymentTermCalculationServiceGrpc.newBlockingStub(channel),
                BudgetCalculationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                events,
                FeatureFlagValidationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SearchIndexValidationServiceGrpc.newBlockingStub(channel),
                TripQueryServiceGrpc.newBlockingStub(channel),
                BankTransferCalculationServiceGrpc.newBlockingStub(channel),
                CashPositionValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ExplainPaymentTermDecisionRequest.getDefaultInstance());
        var response = application.explainPaymentTermDecision(request);
        var sent0 = channel.request("acme.billing.v1.PaymentTermCalculationService/ExplainPaymentTermCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "name"),
                ProbeSupport.value(sent0, "details", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "payment_term", "audit", "updated_by"),
                ProbeSupport.value(sent0, "payment_term", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "payment_term", "discount", "amount"),
                ProbeSupport.value(sent0, "payment_term", "discount", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "email"),
                ProbeSupport.value(sent0, "details", "contact", "email"));
        var sent1 = channel.request("acme.search.v1.SearchIndexValidationService/PreviewSearchIndex");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.PaymentTermCalculationService/ExplainPaymentTermCalculation"), "payment_term", "owner", "user_id"),
                ProbeSupport.value(sent1, "search_index", "owner", "user_id"));
        var sent2 = channel.request("acme.travel.v1.TripQueryService/SuggestTrips");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.search.v1.SearchIndexValidationService/PreviewSearchIndex"), "search_index", "address", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.PaymentTermCalculationService/ExplainPaymentTermCalculation"), "payment_term", "audit", "updated_at"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "payment_term", "owner", "user_id"),
                ProbeSupport.value(sent2, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.PaymentTermCalculationService/ExplainPaymentTermCalculation"), "payment_term", "parameters"),
                ProbeSupport.value(response, "payment_term", "parameters"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.search.v1.SearchIndexValidationService/PreviewSearchIndex"), "search_index", "name"),
                ProbeSupport.value(response, "payment_term", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.search.v1.SearchIndexValidationService/PreviewSearchIndex"), "outcome", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.travel.v1.TripQueryService/SuggestTrips"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "payment_term_id"),
                ProbeSupport.value(response, "payment_term", "payment_term_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "payment_term", "version"),
                ProbeSupport.value(response, "payment_term", "version"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new PaymentTermValidationApplicationEvents(PaymentTermCalculationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<RecalculatePaymentTermsResponse>();
        listener.on(new PaymentTermValidationApplication.ScorePaymentTermStage0Requested(ProbeSupport.sample(RecalculatePaymentTermsRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
