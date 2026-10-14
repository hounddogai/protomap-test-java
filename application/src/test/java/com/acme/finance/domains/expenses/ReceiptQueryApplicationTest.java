package com.acme.finance.domains.expenses;

import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.audit.v1.CheckAuditTrailEligibilityRequest;
import com.acme.audit.v1.CheckAuditTrailEligibilityResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.concessions.v1.ConcessionStand;
import com.acme.concessions.v1.ConcessionStandCalculationServiceGrpc;
import com.acme.concessions.v1.RecalculateConcessionStandsRequest;
import com.acme.concessions.v1.RecalculateConcessionStandsResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.documents.v1.DocumentServiceGrpc;
import com.acme.documents.v1.WatchDocumentsRequest;
import com.acme.documents.v1.WatchDocumentsResponse;
import com.acme.expenses.v1.BrowseReceiptsRequest;
import com.acme.expenses.v1.BrowseReceiptsResponse;
import com.acme.expenses.v1.CalculateExpensePolicyRequest;
import com.acme.expenses.v1.CalculateExpensePolicyResponse;
import com.acme.expenses.v1.CountReceiptsRequest;
import com.acme.expenses.v1.CountReceiptsResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicyCalculationServiceGrpc;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportBankSettlement;
import com.acme.expenses.v1.ExpenseReportCalculationServiceGrpc;
import com.acme.expenses.v1.ExpenseReportValidationServiceGrpc;
import com.acme.expenses.v1.ExportReceiptsRequest;
import com.acme.expenses.v1.ExportReceiptsResponse;
import com.acme.expenses.v1.FindReceiptsRequest;
import com.acme.expenses.v1.FindReceiptsResponse;
import com.acme.expenses.v1.LookupReceiptRequest;
import com.acme.expenses.v1.LookupReceiptResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.PeekReceiptRequest;
import com.acme.expenses.v1.PeekReceiptResponse;
import com.acme.expenses.v1.RecalculateExpenseReportsRequest;
import com.acme.expenses.v1.RecalculateExpenseReportsResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.SuggestReceiptsRequest;
import com.acme.expenses.v1.SuggestReceiptsResponse;
import com.acme.expenses.v1.SummarizeReceiptsRequest;
import com.acme.expenses.v1.SummarizeReceiptsResponse;
import com.acme.expenses.v1.ValidateExpenseReportRequest;
import com.acme.expenses.v1.ValidateExpenseReportResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchHistoryServiceGrpc;
import com.acme.ledger.v1.RecordPostingBatchVersionRequest;
import com.acme.ledger.v1.RecordPostingBatchVersionResponse;
import com.acme.venues.v1.SearchVenuesRequest;
import com.acme.venues.v1.SearchVenuesResponse;
import com.acme.venues.v1.VenueServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class ReceiptQueryApplicationTest {
    @Test
    void exportReceiptsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ExpensesRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExpensePolicy(String tenant, ExpensePolicy value) {}
            public void saveExpenseReport(String tenant, ExpenseReport value) {}
            public void saveMileageClaim(String tenant, MileageClaim value) {}
            public void saveReceipt(String tenant, Receipt value) {}
        };
        var listener = new ReceiptQueryApplicationEvents(ExpenseReportCalculationServiceGrpc.newStub(channel), VenueServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof ReceiptQueryApplication.FindReceiptsStage0Requested command) {
                listener.on(command);
            }
            else if (event instanceof ReceiptQueryApplication.SummarizeReceiptsStage2Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new ReceiptQueryApplication(
                AuditTrailValidationServiceGrpc.newFutureStub(channel),
                ConcessionStandCalculationServiceGrpc.newBlockingStub(channel),
                DocumentServiceGrpc.newBlockingStub(channel),
                events,
                ExpensePolicyCalculationServiceGrpc.newBlockingStub(channel),
                ExpenseReportValidationServiceGrpc.newBlockingStub(channel),
                PostingBatchHistoryServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(ExportReceiptsRequest.getDefaultInstance());
        var response = application.exportReceipts(request);
        var sent0 = channel.request("acme.expenses.v1.ExpensePolicyCalculationService/CalculateExpensePolicy");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent0, "details", "destination", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent0, "expense_policy", "owner", "email"));
        var sent1 = channel.request("acme.documents.v1.DocumentService/WatchDocuments");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpensePolicyCalculationService/CalculateExpensePolicy"), "expense_policy", "owner", "email"),
                ProbeSupport.value(sent1, "subscriber", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "owner_id"),
                ProbeSupport.value(sent1, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpensePolicyCalculationService/CalculateExpensePolicy"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpensePolicyCalculationService/CalculateExpensePolicy"), "breakdown", "total"),
                ProbeSupport.value(response, "receipt", "total"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(response, "receipt", "billing_address", "region"));
        var endpoint = new ReceiptQueryEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<ExportReceiptsResponse> observer = new StreamObserver<>() {
            public void onNext(ExportReceiptsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.exportReceipts(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void summarizeReceiptsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ExpensesRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExpensePolicy(String tenant, ExpensePolicy value) {}
            public void saveExpenseReport(String tenant, ExpenseReport value) {}
            public void saveMileageClaim(String tenant, MileageClaim value) {}
            public void saveReceipt(String tenant, Receipt value) {}
        };
        var listener = new ReceiptQueryApplicationEvents(ExpenseReportCalculationServiceGrpc.newStub(channel), VenueServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof ReceiptQueryApplication.FindReceiptsStage0Requested command) {
                listener.on(command);
            }
            else if (event instanceof ReceiptQueryApplication.SummarizeReceiptsStage2Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new ReceiptQueryApplication(
                AuditTrailValidationServiceGrpc.newFutureStub(channel),
                ConcessionStandCalculationServiceGrpc.newBlockingStub(channel),
                DocumentServiceGrpc.newBlockingStub(channel),
                events,
                ExpensePolicyCalculationServiceGrpc.newBlockingStub(channel),
                ExpenseReportValidationServiceGrpc.newBlockingStub(channel),
                PostingBatchHistoryServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(SummarizeReceiptsRequest.getDefaultInstance());
        var response = application.summarizeReceipts(request);
        var sent0 = channel.request("acme.audit.v1.AuditTrailValidationService/CheckAuditTrailEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent0, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent0, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "locale"),
                ProbeSupport.value(sent0, "details", "locale"));
        var sent1 = channel.request("acme.expenses.v1.ExpenseReportValidationService/ValidateExpenseReport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailValidationService/CheckAuditTrailEligibility"), "audit_trail", "billing_address", "region"),
                ProbeSupport.value(sent1, "expense_report", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailValidationService/CheckAuditTrailEligibility"), "audit_trail", "billing_address", "country_code"),
                ProbeSupport.value(sent1, "expense_report", "billing_address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailValidationService/CheckAuditTrailEligibility"), "audit_trail", "bank", "bic"),
                ProbeSupport.value(sent1, "expense_report", "bank", "bic"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailValidationService/CheckAuditTrailEligibility"), "audit_trail", "bank", "iban"),
                ProbeSupport.value(sent1, "expense_report", "bank", "iban"));
        var sent2 = channel.request("acme.venues.v1.VenueService/SearchVenues");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpenseReportValidationService/ValidateExpenseReport"), "expense_report", "billing_address", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailValidationService/CheckAuditTrailEligibility"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailValidationService/CheckAuditTrailEligibility"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpenseReportValidationService/ValidateExpenseReport"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "period"),
                ProbeSupport.value(response, "period"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new ReceiptQueryApplicationEvents(ExpenseReportCalculationServiceGrpc.newStub(channel), VenueServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<RecalculateExpenseReportsResponse>();
        listener.on(new ReceiptQueryApplication.FindReceiptsStage0Requested(ProbeSupport.sample(RecalculateExpenseReportsRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<SearchVenuesResponse>();
        listener.on(new ReceiptQueryApplication.SummarizeReceiptsStage2Requested(ProbeSupport.sample(SearchVenuesRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
    }
}
