package com.acme.finance.domains.financeportal;

import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalSyncServiceGrpc;
import com.acme.assets.v1.StreamDisposalChangesRequest;
import com.acme.assets.v1.StreamDisposalChangesResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.PreviewAuditEventRequest;
import com.acme.audit.v1.PreviewAuditEventResponse;
import com.acme.audit.v1.RepairRetentionPolicyRequest;
import com.acme.audit.v1.RepairRetentionPolicyResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyAdminServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdWorkflowServiceGrpc;
import com.acme.customers.v1.PauseHouseholdWorkflowRequest;
import com.acme.customers.v1.PauseHouseholdWorkflowResponse;
import com.acme.edgegateway.v1.RouteBindingServiceGrpc;
import com.acme.edgegateway.v1.WatchRouteBindingsRequest;
import com.acme.edgegateway.v1.WatchRouteBindingsResponse;
import com.acme.fanportal.v1.FanSessionServiceGrpc;
import com.acme.fanportal.v1.ListFanSessionsRequest;
import com.acme.fanportal.v1.ListFanSessionsResponse;
import com.acme.featureflags.v1.BrowseFeatureFlagsRequest;
import com.acme.featureflags.v1.BrowseFeatureFlagsResponse;
import com.acme.featureflags.v1.FeatureFlagQueryServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.financeportal.v1.AcknowledgeCloseChecklistEventRequest;
import com.acme.financeportal.v1.AcknowledgeCloseChecklistEventResponse;
import com.acme.financeportal.v1.ApprovalInbox;
import com.acme.financeportal.v1.ApprovalInboxNotificationServiceGrpc;
import com.acme.financeportal.v1.BrowseCloseChecklistsRequest;
import com.acme.financeportal.v1.BrowseCloseChecklistsResponse;
import com.acme.financeportal.v1.CancelCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.CancelCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.CloseChecklist;
import com.acme.financeportal.v1.CloseChecklistIntegrationServiceGrpc;
import com.acme.financeportal.v1.CloseChecklistQueryServiceGrpc;
import com.acme.financeportal.v1.CloseChecklistWorkflowServiceGrpc;
import com.acme.financeportal.v1.ComposeFinanceWorkspacePageRequest;
import com.acme.financeportal.v1.ComposeFinanceWorkspacePageResponse;
import com.acme.financeportal.v1.ConnectFinanceWorkspaceSourceRequest;
import com.acme.financeportal.v1.ConnectFinanceWorkspaceSourceResponse;
import com.acme.financeportal.v1.FetchFinanceWorkspaceViewRequest;
import com.acme.financeportal.v1.FetchFinanceWorkspaceViewResponse;
import com.acme.financeportal.v1.FinanceWorkspace;
import com.acme.financeportal.v1.FinanceWorkspaceIntegrationServiceGrpc;
import com.acme.financeportal.v1.FinanceWorkspaceNotificationServiceGrpc;
import com.acme.financeportal.v1.FinanceWorkspaceServiceGrpc;
import com.acme.financeportal.v1.GetFinanceWorkspaceSummaryRequest;
import com.acme.financeportal.v1.GetFinanceWorkspaceSummaryResponse;
import com.acme.financeportal.v1.ListFinanceWorkspaceSubscriptionsRequest;
import com.acme.financeportal.v1.ListFinanceWorkspaceSubscriptionsResponse;
import com.acme.financeportal.v1.NotifyFinanceWorkspaceWatchersRequest;
import com.acme.financeportal.v1.NotifyFinanceWorkspaceWatchersResponse;
import com.acme.financeportal.v1.RefreshFinanceWorkspaceRequest;
import com.acme.financeportal.v1.RefreshFinanceWorkspaceResponse;
import com.acme.financeportal.v1.ResolveCloseChecklistsRequest;
import com.acme.financeportal.v1.ResolveCloseChecklistsResponse;
import com.acme.financeportal.v1.RetryCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.RetryCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.SearchFinanceWorkspaceCatalogRequest;
import com.acme.financeportal.v1.SearchFinanceWorkspaceCatalogResponse;
import com.acme.financeportal.v1.SignalCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.SignalCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.StartFinanceWorkspaceCheckoutRequest;
import com.acme.financeportal.v1.StartFinanceWorkspaceCheckoutResponse;
import com.acme.financeportal.v1.StreamFinanceWorkspaceUpdatesRequest;
import com.acme.financeportal.v1.StreamFinanceWorkspaceUpdatesResponse;
import com.acme.financeportal.v1.SubmitFinanceWorkspaceFormRequest;
import com.acme.financeportal.v1.SubmitFinanceWorkspaceFormResponse;
import com.acme.financeportal.v1.TrackFinanceWorkspaceRequest;
import com.acme.financeportal.v1.TrackFinanceWorkspaceResponse;
import com.acme.financeportal.v1.UnsubscribeApprovalInboxUpdatesRequest;
import com.acme.financeportal.v1.UnsubscribeApprovalInboxUpdatesResponse;
import com.acme.financeportal.v1.UploadFinanceWorkspaceAttachmentsRequest;
import com.acme.financeportal.v1.UploadFinanceWorkspaceAttachmentsResponse;
import com.acme.financeportal.v1.WatchFinanceWorkspacesRequest;
import com.acme.financeportal.v1.WatchFinanceWorkspacesResponse;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ListUnderwritingConflictsRequest;
import com.acme.insurance.v1.ListUnderwritingConflictsResponse;
import com.acme.insurance.v1.PolicyLifecycleServiceGrpc;
import com.acme.insurance.v1.ReconcileUnderwritingsRequest;
import com.acme.insurance.v1.ReconcileUnderwritingsResponse;
import com.acme.insurance.v1.TerminatePolicyRequest;
import com.acme.insurance.v1.TerminatePolicyResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingStatusChange;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.ledger.v1.ExplainLedgerAccountDecisionRequest;
import com.acme.ledger.v1.ExplainLedgerAccountDecisionResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.procurement.v1.EscalateGoodsReceiptRequest;
import com.acme.procurement.v1.EscalateGoodsReceiptResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.GoodsReceiptApprovalServiceGrpc;
import com.acme.procurement.v1.GoodsReceiptWorkflowServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionRef;
import com.acme.procurement.v1.RejectGoodsReceiptRequest;
import com.acme.procurement.v1.RejectGoodsReceiptResponse;
import com.acme.procurement.v1.SignalGoodsReceiptWorkflowRequest;
import com.acme.procurement.v1.SignalGoodsReceiptWorkflowResponse;
import com.acme.returns.v1.ReturnRequestServiceGrpc;
import com.acme.returns.v1.WatchReturnRequestsRequest;
import com.acme.returns.v1.WatchReturnRequestsResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.acme.search.v1.SummarizeSearchIndexesRequest;
import com.acme.search.v1.SummarizeSearchIndexesResponse;
import com.acme.suppliers.v1.BatchCreateSuppliersRequest;
import com.acme.suppliers.v1.BatchCreateSuppliersResponse;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
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

class FinanceWorkspaceGatewayApplicationTest {
    @Test
    void streamFinanceWorkspaceUpdatesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new FinanceportalRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveApprovalInbox(String tenant, ApprovalInbox value) {}
            public void saveCloseChecklist(String tenant, CloseChecklist value) {}
            public void saveFinanceWorkspace(String tenant, FinanceWorkspace value) {}
        };
        var listener = new FinanceWorkspaceGatewayApplicationEvents(GoodsReceiptWorkflowServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof FinanceWorkspaceGatewayApplication.ComposeFinanceWorkspacePageStage2Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new FinanceWorkspaceGatewayApplication(
                DisposalSyncServiceGrpc.newStub(channel),
                AuditEventValidationServiceGrpc.newBlockingStub(channel),
                RetentionPolicyAdminServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                HouseholdWorkflowServiceGrpc.newBlockingStub(channel),
                RouteBindingServiceGrpc.newBlockingStub(channel),
                events,
                FanSessionServiceGrpc.newBlockingStub(channel),
                FeatureFlagQueryServiceGrpc.newBlockingStub(channel),
                ApprovalInboxNotificationServiceGrpc.newBlockingStub(channel),
                CloseChecklistIntegrationServiceGrpc.newBlockingStub(channel),
                CloseChecklistQueryServiceGrpc.newBlockingStub(channel),
                CloseChecklistWorkflowServiceGrpc.newBlockingStub(channel),
                FinanceWorkspaceIntegrationServiceGrpc.newBlockingStub(channel),
                FinanceWorkspaceNotificationServiceGrpc.newBlockingStub(channel),
                FinanceWorkspaceServiceGrpc.newBlockingStub(channel),
                PolicyLifecycleServiceGrpc.newBlockingStub(channel),
                UnderwritingSyncServiceGrpc.newBlockingStub(channel),
                new FinanceWorkspaceGatewayRequestMapperImpl(),
                GoodsReceiptApprovalServiceGrpc.newBlockingStub(channel),
                repository,
                ReturnRequestServiceGrpc.newBlockingStub(channel),
                Runnable::run,
                SupplierServiceGrpc.newStub(channel));
        var request = ProbeSupport.sample(StreamFinanceWorkspaceUpdatesRequest.getDefaultInstance());
        var response = application.streamFinanceWorkspaceUpdates(request);
        var sent0 = channel.request("acme.financeportal.v1.FinanceWorkspaceNotificationService/ListFinanceWorkspaceSubscriptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "owner_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        var sent1 = channel.request("acme.featureflags.v1.FeatureFlagQueryService/BrowseFeatureFlags");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "owner_id"),
                ProbeSupport.value(sent1, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "labels"),
                ProbeSupport.value(sent1, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.FinanceWorkspaceNotificationService/ListFinanceWorkspaceSubscriptions"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.featureflags.v1.FeatureFlagQueryService/BrowseFeatureFlags"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(response, "finance_workspace", "assignee", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "phone"),
                ProbeSupport.value(response, "finance_workspace", "requester", "phone"));
        var endpoint = new FinanceWorkspaceGatewayEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<StreamFinanceWorkspaceUpdatesResponse> observer = new StreamObserver<>() {
            public void onNext(StreamFinanceWorkspaceUpdatesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.streamFinanceWorkspaceUpdates(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void uploadFinanceWorkspaceAttachmentsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new FinanceportalRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveApprovalInbox(String tenant, ApprovalInbox value) {}
            public void saveCloseChecklist(String tenant, CloseChecklist value) {}
            public void saveFinanceWorkspace(String tenant, FinanceWorkspace value) {}
        };
        var listener = new FinanceWorkspaceGatewayApplicationEvents(GoodsReceiptWorkflowServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof FinanceWorkspaceGatewayApplication.ComposeFinanceWorkspacePageStage2Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new FinanceWorkspaceGatewayApplication(
                DisposalSyncServiceGrpc.newStub(channel),
                AuditEventValidationServiceGrpc.newBlockingStub(channel),
                RetentionPolicyAdminServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                HouseholdWorkflowServiceGrpc.newBlockingStub(channel),
                RouteBindingServiceGrpc.newBlockingStub(channel),
                events,
                FanSessionServiceGrpc.newBlockingStub(channel),
                FeatureFlagQueryServiceGrpc.newBlockingStub(channel),
                ApprovalInboxNotificationServiceGrpc.newBlockingStub(channel),
                CloseChecklistIntegrationServiceGrpc.newBlockingStub(channel),
                CloseChecklistQueryServiceGrpc.newBlockingStub(channel),
                CloseChecklistWorkflowServiceGrpc.newBlockingStub(channel),
                FinanceWorkspaceIntegrationServiceGrpc.newBlockingStub(channel),
                FinanceWorkspaceNotificationServiceGrpc.newBlockingStub(channel),
                FinanceWorkspaceServiceGrpc.newBlockingStub(channel),
                PolicyLifecycleServiceGrpc.newBlockingStub(channel),
                UnderwritingSyncServiceGrpc.newBlockingStub(channel),
                new FinanceWorkspaceGatewayRequestMapperImpl(),
                GoodsReceiptApprovalServiceGrpc.newBlockingStub(channel),
                repository,
                ReturnRequestServiceGrpc.newBlockingStub(channel),
                Runnable::run,
                SupplierServiceGrpc.newStub(channel));
        var request = ProbeSupport.sample(UploadFinanceWorkspaceAttachmentsRequest.getDefaultInstance());
        var response = application.uploadFinanceWorkspaceAttachments(request);
        var sent0 = channel.request("acme.insurance.v1.UnderwritingSyncService/ReconcileUnderwritings");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "history", "changed_at"),
                ProbeSupport.value(sent0, "underwriting", "history", "changed_at"));
        var sent1 = channel.request("acme.financeportal.v1.CloseChecklistWorkflowService/RetryCloseChecklistWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "comments", "author_email"),
                ProbeSupport.value(sent1, "close_checklist", "counterparty", "email"));
        var sent2 = channel.request("acme.financeportal.v1.CloseChecklistWorkflowService/SignalCloseChecklistWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingSyncService/ReconcileUnderwritings"), "underwriting", "customer", "display_name"),
                ProbeSupport.value(sent2, "close_checklist", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.CloseChecklistWorkflowService/RetryCloseChecklistWorkflow"), "close_checklist", "audit", "etag"),
                ProbeSupport.value(sent2, "close_checklist", "audit", "etag"));
        var sent3 = channel.request("acme.audit.v1.RetentionPolicyAdminService/RepairRetentionPolicy");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingSyncService/ReconcileUnderwritings"), "underwriting", "customer", "display_name"),
                ProbeSupport.value(sent3, "retention_policy", "owner", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.CloseChecklistWorkflowService/SignalCloseChecklistWorkflow"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyAdminService/RepairRetentionPolicy"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyAdminService/RepairRetentionPolicy"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        var endpoint = new FinanceWorkspaceGatewayEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<UploadFinanceWorkspaceAttachmentsResponse> observer = new StreamObserver<>() {
            public void onNext(UploadFinanceWorkspaceAttachmentsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        var input = endpoint.uploadFinanceWorkspaceAttachments(observer);
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
        var listener = new FinanceWorkspaceGatewayApplicationEvents(GoodsReceiptWorkflowServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<SignalGoodsReceiptWorkflowResponse>();
        listener.on(new FinanceWorkspaceGatewayApplication.ComposeFinanceWorkspacePageStage2Requested(ProbeSupport.sample(SignalGoodsReceiptWorkflowRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
