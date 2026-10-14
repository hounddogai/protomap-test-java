package com.acme.finance.domains.billing;

import com.acme.billing.v1.BatchCreateBillingAccountsRequest;
import com.acme.billing.v1.BatchCreateBillingAccountsResponse;
import com.acme.billing.v1.BatchGetBillingAccountsRequest;
import com.acme.billing.v1.BatchGetBillingAccountsResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.BillingAccountQueryServiceGrpc;
import com.acme.billing.v1.BrowseInvoicesRequest;
import com.acme.billing.v1.BrowseInvoicesResponse;
import com.acme.billing.v1.CalculateInvoiceRequest;
import com.acme.billing.v1.CalculateInvoiceResponse;
import com.acme.billing.v1.CheckDunningEligibilityRequest;
import com.acme.billing.v1.CheckDunningEligibilityResponse;
import com.acme.billing.v1.CloneBillingAccountRequest;
import com.acme.billing.v1.CloneBillingAccountResponse;
import com.acme.billing.v1.CountBillingAccountsRequest;
import com.acme.billing.v1.CountBillingAccountsResponse;
import com.acme.billing.v1.CreateBillingAccountRequest;
import com.acme.billing.v1.CreateBillingAccountResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.Dunning;
import com.acme.billing.v1.DunningValidationServiceGrpc;
import com.acme.billing.v1.GetBillingAccountRequest;
import com.acme.billing.v1.GetBillingAccountResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceBankSettlement;
import com.acme.billing.v1.InvoiceCalculationServiceGrpc;
import com.acme.billing.v1.InvoiceQueryServiceGrpc;
import com.acme.billing.v1.InvoiceRef;
import com.acme.billing.v1.ListBillingAccountsRequest;
import com.acme.billing.v1.ListBillingAccountsResponse;
import com.acme.billing.v1.PaymentTerm;
import com.acme.billing.v1.PaymentTermCalculationServiceGrpc;
import com.acme.billing.v1.PaymentTermValidationServiceGrpc;
import com.acme.billing.v1.QuotePaymentTermRequest;
import com.acme.billing.v1.QuotePaymentTermResponse;
import com.acme.billing.v1.ResolveBillingAccountsRequest;
import com.acme.billing.v1.ResolveBillingAccountsResponse;
import com.acme.billing.v1.ScorePaymentTermRequest;
import com.acme.billing.v1.ScorePaymentTermResponse;
import com.acme.billing.v1.UpdateBillingAccountRequest;
import com.acme.billing.v1.UpdateBillingAccountResponse;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineValidationServiceGrpc;
import com.acme.budgeting.v1.PreviewVarianceRequest;
import com.acme.budgeting.v1.PreviewVarianceResponse;
import com.acme.budgeting.v1.ScoreBudgetLineRequest;
import com.acme.budgeting.v1.ScoreBudgetLineResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceStatusChange;
import com.acme.budgeting.v1.VarianceValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.risk.v1.BrowseRiskRegistersRequest;
import com.acme.risk.v1.BrowseRiskRegistersResponse;
import com.acme.risk.v1.RiskRegisterQueryServiceGrpc;
import com.acme.suppliers.v1.ListSuppliersRequest;
import com.acme.suppliers.v1.ListSuppliersResponse;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class BillingAccountApplicationTest {
    @Test
    void batchCreateBillingAccountsPreservesPlannedFields() {
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
        var listener = new BillingAccountApplicationEvents(PaymentTermCalculationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof BillingAccountApplication.BatchCreateBillingAccountsStage2Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new BillingAccountApplication(
                BillingAccountQueryServiceGrpc.newBlockingStub(channel),
                DunningValidationServiceGrpc.newBlockingStub(channel),
                InvoiceCalculationServiceGrpc.newBlockingStub(channel),
                InvoiceQueryServiceGrpc.newBlockingStub(channel),
                PaymentTermValidationServiceGrpc.newBlockingStub(channel),
                BudgetLineValidationServiceGrpc.newBlockingStub(channel),
                VarianceValidationServiceGrpc.newBlockingStub(channel),
                events,
                repository,
                RiskRegisterQueryServiceGrpc.newBlockingStub(channel),
                Runnable::run,
                SupplierServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(BatchCreateBillingAccountsRequest.getDefaultInstance());
        var response = application.batchCreateBillingAccounts(request);
        var sent0 = channel.request("acme.risk.v1.RiskRegisterQueryService/BrowseRiskRegisters");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "labels"),
                ProbeSupport.value(sent0, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "audit", "updated_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "holder", "invoice_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        var sent1 = channel.request("acme.billing.v1.BillingAccountQueryService/CountBillingAccounts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "labels"),
                ProbeSupport.value(sent1, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "audit", "updated_at"),
                ProbeSupport.value(sent1, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "holder", "invoice_id"),
                ProbeSupport.value(sent1, "filter", "owner_id"));
        var sent2 = channel.request("acme.billing.v1.PaymentTermCalculationService/QuotePaymentTerm");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "audit", "created_at"),
                ProbeSupport.value(sent2, "payment_term", "audit", "created_at"));
        var sent3 = channel.request("acme.billing.v1.InvoiceCalculationService/CalculateInvoice");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "audit", "updated_at"),
                ProbeSupport.value(sent3, "invoice", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "bank", "iban"),
                ProbeSupport.value(sent3, "invoice", "bank", "iban"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "audit", "created_at"),
                ProbeSupport.value(sent3, "invoice", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskRegisterQueryService/BrowseRiskRegisters"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskRegisterQueryService/BrowseRiskRegisters"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceCalculationService/CalculateInvoice"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        var endpoint = new BillingAccountEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<BatchCreateBillingAccountsResponse> observer = new StreamObserver<>() {
            public void onNext(BatchCreateBillingAccountsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        var input = endpoint.batchCreateBillingAccounts(observer);
        input.onNext(request);
        input.onNext(request);
        input.onCompleted();
        input.onCompleted();
        input.onNext(request);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new BillingAccountApplicationEvents(PaymentTermCalculationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<QuotePaymentTermResponse>();
        listener.on(new BillingAccountApplication.BatchCreateBillingAccountsStage2Requested(ProbeSupport.sample(QuotePaymentTermRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
