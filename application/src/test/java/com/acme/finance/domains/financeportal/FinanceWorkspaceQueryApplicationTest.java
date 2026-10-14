package com.acme.finance.domains.financeportal;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.ListRetentionPolicyConflictsRequest;
import com.acme.audit.v1.ListRetentionPolicyConflictsResponse;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.driverapp.v1.CloneDriverHomeRequest;
import com.acme.driverapp.v1.CloneDriverHomeResponse;
import com.acme.driverapp.v1.DriverHome;
import com.acme.driverapp.v1.DriverHomeServiceGrpc;
import com.acme.featureflags.v1.BatchGetFeatureFlagsRequest;
import com.acme.featureflags.v1.BatchGetFeatureFlagsResponse;
import com.acme.featureflags.v1.CreateFeatureFlagRequest;
import com.acme.featureflags.v1.CreateFeatureFlagResponse;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.financeportal.v1.AcknowledgeCloseChecklistEventRequest;
import com.acme.financeportal.v1.AcknowledgeCloseChecklistEventResponse;
import com.acme.financeportal.v1.ApprovalInbox;
import com.acme.financeportal.v1.BrowseFinanceWorkspacesRequest;
import com.acme.financeportal.v1.BrowseFinanceWorkspacesResponse;
import com.acme.financeportal.v1.CloseChecklist;
import com.acme.financeportal.v1.CloseChecklistIntegrationServiceGrpc;
import com.acme.financeportal.v1.CloseChecklistLine;
import com.acme.financeportal.v1.CloseChecklistWorkflowServiceGrpc;
import com.acme.financeportal.v1.CountFinanceWorkspacesRequest;
import com.acme.financeportal.v1.CountFinanceWorkspacesResponse;
import com.acme.financeportal.v1.DisconnectCloseChecklistSourceRequest;
import com.acme.financeportal.v1.DisconnectCloseChecklistSourceResponse;
import com.acme.financeportal.v1.ExportFinanceWorkspacesRequest;
import com.acme.financeportal.v1.ExportFinanceWorkspacesResponse;
import com.acme.financeportal.v1.FinanceWorkspace;
import com.acme.financeportal.v1.LookupFinanceWorkspaceRequest;
import com.acme.financeportal.v1.LookupFinanceWorkspaceResponse;
import com.acme.financeportal.v1.PauseCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.PauseCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.PeekFinanceWorkspaceRequest;
import com.acme.financeportal.v1.PeekFinanceWorkspaceResponse;
import com.acme.financeportal.v1.RelayCloseChecklistMessagesRequest;
import com.acme.financeportal.v1.RelayCloseChecklistMessagesResponse;
import com.acme.financeportal.v1.ResolveFinanceWorkspacesRequest;
import com.acme.financeportal.v1.ResolveFinanceWorkspacesResponse;
import com.acme.financeportal.v1.SummarizeFinanceWorkspacesRequest;
import com.acme.financeportal.v1.SummarizeFinanceWorkspacesResponse;
import com.acme.financeportal.v1.TestCloseChecklistConnectionRequest;
import com.acme.financeportal.v1.TestCloseChecklistConnectionResponse;
import com.acme.identity.v1.BatchGetUsersRequest;
import com.acme.identity.v1.BatchGetUsersResponse;
import com.acme.identity.v1.CreateUserRequest;
import com.acme.identity.v1.CreateUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.procurement.v1.PurchaseOrderWorkflowServiceGrpc;
import com.acme.procurement.v1.SignalPurchaseOrderWorkflowRequest;
import com.acme.procurement.v1.SignalPurchaseOrderWorkflowResponse;
import com.acme.suppliers.v1.EscalateSupplierRatingRequest;
import com.acme.suppliers.v1.EscalateSupplierRatingResponse;
import com.acme.suppliers.v1.SupplierRatingApprovalServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class FinanceWorkspaceQueryApplicationTest {
    @Test
    void exportFinanceWorkspacesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new FinanceportalRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveApprovalInbox(String tenant, ApprovalInbox value) {}
            public void saveCloseChecklist(String tenant, CloseChecklist value) {}
            public void saveFinanceWorkspace(String tenant, FinanceWorkspace value) {}
        };
        var application = new FinanceWorkspaceQueryApplication(
                AuditEventServiceGrpc.newStub(channel),
                RetentionPolicySyncServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                DriverHomeServiceGrpc.newBlockingStub(channel),
                FeatureFlagServiceGrpc.newBlockingStub(channel),
                CloseChecklistIntegrationServiceGrpc.newStub(channel),
                CloseChecklistIntegrationServiceGrpc.newBlockingStub(channel),
                CloseChecklistIntegrationServiceGrpc.newFutureStub(channel),
                UserServiceGrpc.newBlockingStub(channel),
                PurchaseOrderWorkflowServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SupplierRatingApprovalServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ExportFinanceWorkspacesRequest.getDefaultInstance());
        var response = application.exportFinanceWorkspaces(request);
        var sent0 = channel.request("acme.financeportal.v1.CloseChecklistWorkflowService/PauseCloseChecklistWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent0, "close_checklist", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent0, "details", "notify", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "created_after"),
                ProbeSupport.value(sent0, "close_checklist", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "labels"),
                ProbeSupport.value(sent0, "close_checklist", "lines", "attributes"));
        var sent1 = channel.request("acme.identity.v1.UserService/CreateUser");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.CloseChecklistWorkflowService/PauseCloseChecklistWorkflow"), "close_checklist", "billing_address", "postal_code"),
                ProbeSupport.value(sent1, "user", "primary_address", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.CloseChecklistWorkflowService/PauseCloseChecklistWorkflow"), "close_checklist", "audit", "etag"),
                ProbeSupport.value(sent1, "user", "audit", "etag"));
        var sent2 = channel.request("acme.financeportal.v1.CloseChecklistIntegrationService/AcknowledgeCloseChecklistEvent");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.CloseChecklistWorkflowService/PauseCloseChecklistWorkflow"), "close_checklist", "close_checklist_id"),
                ProbeSupport.value(sent2, "close_checklist_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.CloseChecklistWorkflowService/PauseCloseChecklistWorkflow"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.identity.v1.UserService/CreateUser"), "user", "audit", "created_at"),
                ProbeSupport.value(response, "finance_workspace", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.CloseChecklistIntegrationService/AcknowledgeCloseChecklistEvent"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        var endpoint = new FinanceWorkspaceQueryEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<ExportFinanceWorkspacesResponse> observer = new StreamObserver<>() {
            public void onNext(ExportFinanceWorkspacesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.exportFinanceWorkspaces(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void summarizeFinanceWorkspacesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new FinanceportalRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveApprovalInbox(String tenant, ApprovalInbox value) {}
            public void saveCloseChecklist(String tenant, CloseChecklist value) {}
            public void saveFinanceWorkspace(String tenant, FinanceWorkspace value) {}
        };
        var application = new FinanceWorkspaceQueryApplication(
                AuditEventServiceGrpc.newStub(channel),
                RetentionPolicySyncServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                DriverHomeServiceGrpc.newBlockingStub(channel),
                FeatureFlagServiceGrpc.newBlockingStub(channel),
                CloseChecklistIntegrationServiceGrpc.newStub(channel),
                CloseChecklistIntegrationServiceGrpc.newBlockingStub(channel),
                CloseChecklistIntegrationServiceGrpc.newFutureStub(channel),
                UserServiceGrpc.newBlockingStub(channel),
                PurchaseOrderWorkflowServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SupplierRatingApprovalServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SummarizeFinanceWorkspacesRequest.getDefaultInstance());
        var response = application.summarizeFinanceWorkspaces(request);
        var sent0 = channel.request("acme.procurement.v1.PurchaseOrderWorkflowService/SignalPurchaseOrderWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent0, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "annotations"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent0, "details", "notify", "email"));
        var sent1 = channel.request("acme.identity.v1.UserService/BatchGetUsers");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        var sent2 = channel.request("acme.financeportal.v1.CloseChecklistIntegrationService/DisconnectCloseChecklistSource");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent2, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent2, "annotations"));
        var sent3 = channel.request("acme.featureflags.v1.FeatureFlagService/CreateFeatureFlag");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseOrderWorkflowService/SignalPurchaseOrderWorkflow"), "purchase_order", "audit", "updated_at"),
                ProbeSupport.value(sent3, "feature_flag", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseOrderWorkflowService/SignalPurchaseOrderWorkflow"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.CloseChecklistIntegrationService/DisconnectCloseChecklistSource"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.CloseChecklistIntegrationService/DisconnectCloseChecklistSource"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "period"),
                ProbeSupport.value(response, "period"));
    }
}
