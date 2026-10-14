package com.acme.finance.domains.financeportal;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.ListAuditEventsRequest;
import com.acme.audit.v1.ListAuditEventsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentApprovalServiceGrpc;
import com.acme.customers.v1.CustomerSegmentBlockAction;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.SimulateCustomerSegmentRequest;
import com.acme.customers.v1.SimulateCustomerSegmentResponse;
import com.acme.customers.v1.SubmitCustomerSegmentRequest;
import com.acme.customers.v1.SubmitCustomerSegmentResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.financeportal.v1.ApprovalInbox;
import com.acme.financeportal.v1.ApprovalInboxLine;
import com.acme.financeportal.v1.ApprovalInboxLineTax;
import com.acme.financeportal.v1.BrowseApprovalInboxesRequest;
import com.acme.financeportal.v1.BrowseApprovalInboxesResponse;
import com.acme.financeportal.v1.CloseChecklist;
import com.acme.financeportal.v1.CloseChecklistLine;
import com.acme.financeportal.v1.CloseChecklistWorkflowServiceGrpc;
import com.acme.financeportal.v1.CountApprovalInboxesRequest;
import com.acme.financeportal.v1.CountApprovalInboxesResponse;
import com.acme.financeportal.v1.ExportApprovalInboxesRequest;
import com.acme.financeportal.v1.ExportApprovalInboxesResponse;
import com.acme.financeportal.v1.FinanceWorkspace;
import com.acme.financeportal.v1.FinanceWorkspaceQueryServiceGrpc;
import com.acme.financeportal.v1.FindApprovalInboxesRequest;
import com.acme.financeportal.v1.FindApprovalInboxesResponse;
import com.acme.financeportal.v1.LookupApprovalInboxRequest;
import com.acme.financeportal.v1.LookupApprovalInboxResponse;
import com.acme.financeportal.v1.PeekApprovalInboxRequest;
import com.acme.financeportal.v1.PeekApprovalInboxResponse;
import com.acme.financeportal.v1.ResolveApprovalInboxesRequest;
import com.acme.financeportal.v1.ResolveApprovalInboxesResponse;
import com.acme.financeportal.v1.ResumeCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.ResumeCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.SuggestApprovalInboxesRequest;
import com.acme.financeportal.v1.SuggestApprovalInboxesResponse;
import com.acme.financeportal.v1.SummarizeApprovalInboxesRequest;
import com.acme.financeportal.v1.SummarizeApprovalInboxesResponse;
import com.acme.financeportal.v1.SummarizeFinanceWorkspacesRequest;
import com.acme.financeportal.v1.SummarizeFinanceWorkspacesResponse;
import com.acme.identity.v1.UserRef;
import com.acme.payroll.v1.GenerateTaxWithholdingReportRequest;
import com.acme.payroll.v1.GenerateTaxWithholdingReportResponse;
import com.acme.payroll.v1.TaxWithholdingReportingServiceGrpc;
import com.acme.venues.v1.SummarizeVenuesRequest;
import com.acme.venues.v1.SummarizeVenuesResponse;
import com.acme.venues.v1.VenueQueryServiceGrpc;
import com.acme.webbff.v1.PageCompositionQueryServiceGrpc;
import com.acme.webbff.v1.ResolvePageCompositionsRequest;
import com.acme.webbff.v1.ResolvePageCompositionsResponse;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class ApprovalInboxQueryApplicationTest {
    @Test
    void exportApprovalInboxesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new FinanceportalRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveApprovalInbox(String tenant, ApprovalInbox value) {}
            public void saveCloseChecklist(String tenant, CloseChecklist value) {}
            public void saveFinanceWorkspace(String tenant, FinanceWorkspace value) {}
        };
        var listener = new ApprovalInboxQueryApplicationEvents(TaxWithholdingReportingServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof ApprovalInboxQueryApplication.PeekApprovalInboxStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new ApprovalInboxQueryApplication(
                AuditEventServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CustomerSegmentCalculationServiceGrpc.newBlockingStub(channel),
                events,
                CloseChecklistWorkflowServiceGrpc.newBlockingStub(channel),
                FinanceWorkspaceQueryServiceGrpc.newBlockingStub(channel),
                new ApprovalInboxQueryRequestMapperImpl(),
                repository,
                VenueQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ExportApprovalInboxesRequest.getDefaultInstance());
        var response = application.exportApprovalInboxes(request);
        var sent0 = channel.request("acme.customers.v1.CustomerSegmentApprovalService/SubmitCustomerSegment");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent0, "details", "approver", "email"));
        var sent1 = channel.request("acme.financeportal.v1.CloseChecklistWorkflowService/ResumeCloseChecklistWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerSegmentApprovalService/SubmitCustomerSegment"), "customer_segment", "audit", "created_by"),
                ProbeSupport.value(sent1, "close_checklist", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerSegmentApprovalService/SubmitCustomerSegment"), "customer_segment", "discount", "amount"),
                ProbeSupport.value(sent1, "close_checklist", "lines", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerSegmentApprovalService/SubmitCustomerSegment"), "customer_segment", "discount", "amount"),
                ProbeSupport.value(response, "approval_inbox", "lines", "tax", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerSegmentApprovalService/SubmitCustomerSegment"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.CloseChecklistWorkflowService/ResumeCloseChecklistWorkflow"), "close_checklist", "audit", "etag"),
                ProbeSupport.value(response, "approval_inbox", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(response, "approval_inbox", "billing_address", "region"));
        var endpoint = new ApprovalInboxQueryEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<ExportApprovalInboxesResponse> observer = new StreamObserver<>() {
            public void onNext(ExportApprovalInboxesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.exportApprovalInboxes(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void summarizeApprovalInboxesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new FinanceportalRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveApprovalInbox(String tenant, ApprovalInbox value) {}
            public void saveCloseChecklist(String tenant, CloseChecklist value) {}
            public void saveFinanceWorkspace(String tenant, FinanceWorkspace value) {}
        };
        var listener = new ApprovalInboxQueryApplicationEvents(TaxWithholdingReportingServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof ApprovalInboxQueryApplication.PeekApprovalInboxStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new ApprovalInboxQueryApplication(
                AuditEventServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CustomerSegmentCalculationServiceGrpc.newBlockingStub(channel),
                events,
                CloseChecklistWorkflowServiceGrpc.newBlockingStub(channel),
                FinanceWorkspaceQueryServiceGrpc.newBlockingStub(channel),
                new ApprovalInboxQueryRequestMapperImpl(),
                repository,
                VenueQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SummarizeApprovalInboxesRequest.getDefaultInstance());
        var response = application.summarizeApprovalInboxes(request);
        var sent0 = channel.request("acme.webbff.v1.PageCompositionQueryService/ResolvePageCompositions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        var sent1 = channel.request("acme.venues.v1.VenueQueryService/SummarizeVenues");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "recipient_emails"),
                ProbeSupport.value(sent1, "details", "recipient_emails"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "filters"),
                ProbeSupport.value(sent1, "details", "filters"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.webbff.v1.PageCompositionQueryService/ResolvePageCompositions"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.venues.v1.VenueQueryService/SummarizeVenues"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.venues.v1.VenueQueryService/SummarizeVenues"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "period"),
                ProbeSupport.value(response, "period"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new ApprovalInboxQueryApplicationEvents(TaxWithholdingReportingServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<GenerateTaxWithholdingReportResponse>();
        listener.on(new ApprovalInboxQueryApplication.PeekApprovalInboxStage0Requested(ProbeSupport.sample(GenerateTaxWithholdingReportRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
