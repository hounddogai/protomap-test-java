package com.acme.finance.domains.billing;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.BatchGetAuditEventsRequest;
import com.acme.audit.v1.BatchGetAuditEventsResponse;
import com.acme.billing.v1.ActivateDunningRequest;
import com.acme.billing.v1.ActivateDunningResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.BillingAccountRef;
import com.acme.billing.v1.BillingAccountServiceGrpc;
import com.acme.billing.v1.BrowseInvoicesRequest;
import com.acme.billing.v1.BrowseInvoicesResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.CreditNoteCalculationServiceGrpc;
import com.acme.billing.v1.Dunning;
import com.acme.billing.v1.DunningComment;
import com.acme.billing.v1.DunningReportingServiceGrpc;
import com.acme.billing.v1.EstimateCreditNoteRequest;
import com.acme.billing.v1.EstimateCreditNoteResponse;
import com.acme.billing.v1.EstimateDunningRequest;
import com.acme.billing.v1.EstimateDunningResponse;
import com.acme.billing.v1.ExpireDunningRequest;
import com.acme.billing.v1.ExpireDunningResponse;
import com.acme.billing.v1.FetchDunningReportRequest;
import com.acme.billing.v1.FetchDunningReportResponse;
import com.acme.billing.v1.GetDunningLifecycleRequest;
import com.acme.billing.v1.GetDunningLifecycleResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceQueryServiceGrpc;
import com.acme.billing.v1.InvoiceRef;
import com.acme.billing.v1.InvoiceValidationServiceGrpc;
import com.acme.billing.v1.ListBillingAccountsRequest;
import com.acme.billing.v1.ListBillingAccountsResponse;
import com.acme.billing.v1.PaymentTerm;
import com.acme.billing.v1.PreviewInvoiceRequest;
import com.acme.billing.v1.PreviewInvoiceResponse;
import com.acme.billing.v1.ReinstateDunningRequest;
import com.acme.billing.v1.ReinstateDunningResponse;
import com.acme.billing.v1.RenewDunningRequest;
import com.acme.billing.v1.RenewDunningResponse;
import com.acme.billing.v1.ScheduleDunningExpiryRequest;
import com.acme.billing.v1.ScheduleDunningExpiryResponse;
import com.acme.billing.v1.SuspendDunningRequest;
import com.acme.billing.v1.SuspendDunningResponse;
import com.acme.billing.v1.TerminateDunningRequest;
import com.acme.billing.v1.TerminateDunningResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.customers.v1.CheckPreferenceEligibilityRequest;
import com.acme.customers.v1.CheckPreferenceEligibilityResponse;
import com.acme.customers.v1.CountCustomersRequest;
import com.acme.customers.v1.CountCustomersResponse;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentBlockAction;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.customers.v1.QuoteCustomerSegmentRequest;
import com.acme.customers.v1.QuoteCustomerSegmentResponse;
import com.acme.customers.v1.RecalculateCustomerSegmentsRequest;
import com.acme.customers.v1.RecalculateCustomerSegmentsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentLifecycleServiceGrpc;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.ScheduleRepaymentExpiryRequest;
import com.acme.lending.v1.ScheduleRepaymentExpiryResponse;
import com.acme.notifications.v1.CreateNotificationRequest;
import com.acme.notifications.v1.CreateNotificationResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.risk.v1.ResumeExposureWorkflowRequest;
import com.acme.risk.v1.ResumeExposureWorkflowResponse;
import com.acme.search.v1.NormalizeSearchIndexRequest;
import com.acme.search.v1.NormalizeSearchIndexResponse;
import com.acme.search.v1.PreviewSearchIndexRequest;
import com.acme.search.v1.PreviewSearchIndexResponse;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexComponent;
import com.acme.search.v1.SearchIndexValidationServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class DunningLifecycleApplicationTest {
    @Test
    void scheduleDunningExpiryPreservesPlannedFields() {
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
        var listener = new DunningLifecycleApplicationEvents(AuditEventServiceGrpc.newStub(channel), DunningReportingServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof DunningLifecycleApplication.ActivateDunningStage0Requested command) {
                listener.on(command);
            }
            else if (event instanceof DunningLifecycleApplication.TerminateDunningStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new DunningLifecycleApplication(
                BillingAccountServiceGrpc.newBlockingStub(channel),
                CreditNoteCalculationServiceGrpc.newBlockingStub(channel),
                InvoiceQueryServiceGrpc.newBlockingStub(channel),
                InvoiceValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CustomerQueryServiceGrpc.newFutureStub(channel),
                CustomerSegmentCalculationServiceGrpc.newBlockingStub(channel),
                CustomerSegmentCalculationServiceGrpc.newFutureStub(channel),
                events,
                RepaymentLifecycleServiceGrpc.newBlockingStub(channel),
                NotificationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SearchIndexValidationServiceGrpc.newBlockingStub(channel),
                SearchIndexValidationServiceGrpc.newFutureStub(channel));
        var request = ProbeSupport.sample(ScheduleDunningExpiryRequest.getDefaultInstance());
        var response = application.scheduleDunningExpiry(request);
        var sent0 = channel.request("acme.billing.v1.InvoiceQueryService/BrowseInvoices");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "dunning", "audit", "created_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "dunning", "customer", "billing_account_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "dunning", "custom_fields"),
                ProbeSupport.value(sent0, "filter", "labels"));
        var sent1 = channel.request("acme.search.v1.SearchIndexValidationService/NormalizeSearchIndex");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "dunning", "assignee", "email"),
                ProbeSupport.value(sent1, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "name"),
                ProbeSupport.value(sent1, "details", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "dunning", "requester", "email"),
                ProbeSupport.value(sent1, "search_index", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "dunning", "audit", "updated_by"),
                ProbeSupport.value(sent1, "search_index", "audit", "updated_by"));
        var sent2 = channel.request("acme.billing.v1.InvoiceValidationService/PreviewInvoice");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "dunning", "audit", "etag"),
                ProbeSupport.value(sent2, "invoice", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "dunning", "customer", "display_name"),
                ProbeSupport.value(sent2, "invoice", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceQueryService/BrowseInvoices"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.search.v1.SearchIndexValidationService/NormalizeSearchIndex"), "search_index", "audit", "etag"),
                ProbeSupport.value(response, "dunning", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceValidationService/PreviewInvoice"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "dunning_id"),
                ProbeSupport.value(response, "dunning", "dunning_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "dunning", "assignee"),
                ProbeSupport.value(response, "dunning", "assignee"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new DunningLifecycleApplicationEvents(AuditEventServiceGrpc.newStub(channel), DunningReportingServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<BatchGetAuditEventsResponse>();
        listener.on(new DunningLifecycleApplication.ActivateDunningStage0Requested(ProbeSupport.sample(BatchGetAuditEventsRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<FetchDunningReportResponse>();
        listener.on(new DunningLifecycleApplication.TerminateDunningStage0Requested(ProbeSupport.sample(FetchDunningReportRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
    }
}
