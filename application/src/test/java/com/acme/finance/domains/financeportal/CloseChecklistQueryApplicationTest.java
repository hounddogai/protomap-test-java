package com.acme.finance.domains.financeportal;

import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalApprovalServiceGrpc;
import com.acme.assets.v1.FixedAssetRef;
import com.acme.assets.v1.RejectDisposalRequest;
import com.acme.assets.v1.RejectDisposalResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CommentHouseholdApprovalRequest;
import com.acme.customers.v1.CommentHouseholdApprovalResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.DelegateHouseholdApprovalRequest;
import com.acme.customers.v1.DelegateHouseholdApprovalResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdApprovalServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.financeportal.v1.ApprovalInbox;
import com.acme.financeportal.v1.ApprovalInboxNotificationServiceGrpc;
import com.acme.financeportal.v1.ApprovalInboxWorkflowServiceGrpc;
import com.acme.financeportal.v1.BatchGetFinanceWorkspacesRequest;
import com.acme.financeportal.v1.BatchGetFinanceWorkspacesResponse;
import com.acme.financeportal.v1.BrowseCloseChecklistsRequest;
import com.acme.financeportal.v1.BrowseCloseChecklistsResponse;
import com.acme.financeportal.v1.CloseChecklist;
import com.acme.financeportal.v1.CloseChecklistIntegrationServiceGrpc;
import com.acme.financeportal.v1.CloseChecklistNotificationServiceGrpc;
import com.acme.financeportal.v1.ExportCloseChecklistsRequest;
import com.acme.financeportal.v1.ExportCloseChecklistsResponse;
import com.acme.financeportal.v1.FinanceWorkspace;
import com.acme.financeportal.v1.FinanceWorkspaceIntegrationServiceGrpc;
import com.acme.financeportal.v1.FinanceWorkspaceServiceGrpc;
import com.acme.financeportal.v1.ListApprovalInboxSubscriptionsRequest;
import com.acme.financeportal.v1.ListApprovalInboxSubscriptionsResponse;
import com.acme.financeportal.v1.ListFinanceWorkspaceSourcesRequest;
import com.acme.financeportal.v1.ListFinanceWorkspaceSourcesResponse;
import com.acme.financeportal.v1.LookupCloseChecklistRequest;
import com.acme.financeportal.v1.LookupCloseChecklistResponse;
import com.acme.financeportal.v1.NotifyCloseChecklistWatchersRequest;
import com.acme.financeportal.v1.NotifyCloseChecklistWatchersResponse;
import com.acme.financeportal.v1.PeekCloseChecklistRequest;
import com.acme.financeportal.v1.PeekCloseChecklistResponse;
import com.acme.financeportal.v1.ReplayCloseChecklistEventsRequest;
import com.acme.financeportal.v1.ReplayCloseChecklistEventsResponse;
import com.acme.financeportal.v1.ReplayFinanceWorkspaceEventsRequest;
import com.acme.financeportal.v1.ReplayFinanceWorkspaceEventsResponse;
import com.acme.financeportal.v1.ResolveCloseChecklistsRequest;
import com.acme.financeportal.v1.ResolveCloseChecklistsResponse;
import com.acme.financeportal.v1.ResumeApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.ResumeApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.SuggestCloseChecklistsRequest;
import com.acme.financeportal.v1.SuggestCloseChecklistsResponse;
import com.acme.financeportal.v1.SummarizeCloseChecklistsRequest;
import com.acme.financeportal.v1.SummarizeCloseChecklistsResponse;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.CloneLedgerAccountRequest;
import com.acme.ledger.v1.CloneLedgerAccountResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountServiceGrpc;
import com.acme.ledger.v1.SearchLedgerAccountsRequest;
import com.acme.ledger.v1.SearchLedgerAccountsResponse;
import com.google.protobuf.Any;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
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
import org.springframework.stereotype.Service;

class CloseChecklistQueryApplicationTest {
    @Test
    void exportCloseChecklistsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new FinanceportalRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveApprovalInbox(String tenant, ApprovalInbox value) {}
            public void saveCloseChecklist(String tenant, CloseChecklist value) {}
            public void saveFinanceWorkspace(String tenant, FinanceWorkspace value) {}
        };
        var application = new CloseChecklistQueryApplication(
                DisposalApprovalServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                HouseholdApprovalServiceGrpc.newBlockingStub(channel),
                ApprovalInboxNotificationServiceGrpc.newBlockingStub(channel),
                ApprovalInboxWorkflowServiceGrpc.newBlockingStub(channel),
                CloseChecklistIntegrationServiceGrpc.newBlockingStub(channel),
                FinanceWorkspaceIntegrationServiceGrpc.newBlockingStub(channel),
                FinanceWorkspaceServiceGrpc.newBlockingStub(channel),
                LedgerAccountServiceGrpc.newBlockingStub(channel),
                LedgerAccountServiceGrpc.newFutureStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(ExportCloseChecklistsRequest.getDefaultInstance());
        var response = application.exportCloseChecklists(request);
        var sent0 = channel.request("acme.financeportal.v1.CloseChecklistIntegrationService/ReplayCloseChecklistEvents");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "created_after"),
                ProbeSupport.value(sent0, "details", "received_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "owner_id"),
                ProbeSupport.value(sent0, "details", "message_id"));
        var sent1 = channel.request("acme.assets.v1.DisposalApprovalService/RejectDisposal");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.CloseChecklistIntegrationService/ReplayCloseChecklistEvents"), "close_checklist", "counterparty", "display_name"),
                ProbeSupport.value(sent1, "disposal", "customer", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.CloseChecklistIntegrationService/ReplayCloseChecklistEvents"), "close_checklist", "audit", "updated_at"),
                ProbeSupport.value(sent1, "disposal", "audit", "updated_at"));
        var sent2 = channel.request("acme.customers.v1.HouseholdApprovalService/CommentHouseholdApproval");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalApprovalService/RejectDisposal"), "disposal", "assignee", "email"),
                ProbeSupport.value(sent2, "household", "children", "children", "manager", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalApprovalService/RejectDisposal"), "disposal", "audit", "updated_by"),
                ProbeSupport.value(sent2, "household", "children", "children", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalApprovalService/RejectDisposal"), "disposal", "customer", "display_name"),
                ProbeSupport.value(sent2, "household", "children", "children", "manager", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent2, "household", "children", "children", "location", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.CloseChecklistIntegrationService/ReplayCloseChecklistEvents"), "close_checklist", "counterparty", "display_name"),
                ProbeSupport.value(response, "close_checklist", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalApprovalService/RejectDisposal"), "disposal", "assignee", "email"),
                ProbeSupport.value(response, "close_checklist", "counterparty", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdApprovalService/CommentHouseholdApproval"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdApprovalService/CommentHouseholdApproval"), "household", "location", "region"),
                ProbeSupport.value(response, "close_checklist", "billing_address", "region"));
        var endpoint = new CloseChecklistQueryEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<ExportCloseChecklistsResponse> observer = new StreamObserver<>() {
            public void onNext(ExportCloseChecklistsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.exportCloseChecklists(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void summarizeCloseChecklistsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new FinanceportalRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveApprovalInbox(String tenant, ApprovalInbox value) {}
            public void saveCloseChecklist(String tenant, CloseChecklist value) {}
            public void saveFinanceWorkspace(String tenant, FinanceWorkspace value) {}
        };
        var application = new CloseChecklistQueryApplication(
                DisposalApprovalServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                HouseholdApprovalServiceGrpc.newBlockingStub(channel),
                ApprovalInboxNotificationServiceGrpc.newBlockingStub(channel),
                ApprovalInboxWorkflowServiceGrpc.newBlockingStub(channel),
                CloseChecklistIntegrationServiceGrpc.newBlockingStub(channel),
                FinanceWorkspaceIntegrationServiceGrpc.newBlockingStub(channel),
                FinanceWorkspaceServiceGrpc.newBlockingStub(channel),
                LedgerAccountServiceGrpc.newBlockingStub(channel),
                LedgerAccountServiceGrpc.newFutureStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(SummarizeCloseChecklistsRequest.getDefaultInstance());
        var response = application.summarizeCloseChecklists(request);
        var sent0 = channel.request("acme.financeportal.v1.ApprovalInboxNotificationService/ListApprovalInboxSubscriptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "filters"),
                ProbeSupport.value(sent0, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "close_checklist_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        var sent1 = channel.request("acme.financeportal.v1.FinanceWorkspaceIntegrationService/ReplayFinanceWorkspaceEvents");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent1, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent1, "annotations"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.ApprovalInboxNotificationService/ListApprovalInboxSubscriptions"), "result", "message"),
                ProbeSupport.value(sent1, "details", "message_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "deadline"),
                ProbeSupport.value(sent1, "details", "received_at"));
        var sent2 = channel.request("acme.financeportal.v1.CloseChecklistNotificationService/NotifyCloseChecklistWatchers");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.FinanceWorkspaceIntegrationService/ReplayFinanceWorkspaceEvents"), "finance_workspace", "requester", "name"),
                ProbeSupport.value(sent2, "details", "recipient", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.ApprovalInboxNotificationService/ListApprovalInboxSubscriptions"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.FinanceWorkspaceIntegrationService/ReplayFinanceWorkspaceEvents"), "outcome", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.FinanceWorkspaceIntegrationService/ReplayFinanceWorkspaceEvents"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.CloseChecklistNotificationService/NotifyCloseChecklistWatchers"), "outcome", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "period"),
                ProbeSupport.value(response, "period"));
    }
}
