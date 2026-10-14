package com.acme.finance.domains.financeportal;

import com.acme.audit.v1.AnnotateRetentionPolicyVersionRequest;
import com.acme.audit.v1.AnnotateRetentionPolicyVersionResponse;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.ListAuditTrailVersionsRequest;
import com.acme.audit.v1.ListAuditTrailVersionsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.audit.v1.RetentionPolicyValidationServiceGrpc;
import com.acme.audit.v1.ScoreRetentionPolicyRequest;
import com.acme.audit.v1.ScoreRetentionPolicyResponse;
import com.acme.billing.v1.CreditNoteWorkflowServiceGrpc;
import com.acme.billing.v1.RetryCreditNoteWorkflowRequest;
import com.acme.billing.v1.RetryCreditNoteWorkflowResponse;
import com.acme.budgeting.v1.ImportVariancesRequest;
import com.acme.budgeting.v1.ImportVariancesResponse;
import com.acme.budgeting.v1.VarianceImportServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageRequest;
import com.acme.customers.v1.ApproveHouseholdRequest;
import com.acme.customers.v1.ApproveHouseholdResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdApprovalServiceGrpc;
import com.acme.edgegateway.v1.CloneRouteBindingRequest;
import com.acme.edgegateway.v1.CloneRouteBindingResponse;
import com.acme.edgegateway.v1.RouteBinding;
import com.acme.edgegateway.v1.RouteBindingServiceGrpc;
import com.acme.fanportal.v1.CountFanSessionsRequest;
import com.acme.fanportal.v1.CountFanSessionsResponse;
import com.acme.fanportal.v1.FanSessionQueryServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.financeportal.v1.AcknowledgeFinanceWorkspaceEventRequest;
import com.acme.financeportal.v1.AcknowledgeFinanceWorkspaceEventResponse;
import com.acme.financeportal.v1.ApprovalInbox;
import com.acme.financeportal.v1.ApprovalInboxIntegrationServiceGrpc;
import com.acme.financeportal.v1.CloseChecklist;
import com.acme.financeportal.v1.FinanceWorkspace;
import com.acme.financeportal.v1.FinanceWorkspaceIntegrationServiceGrpc;
import com.acme.financeportal.v1.FinanceWorkspaceSla;
import com.acme.financeportal.v1.GetFinanceWorkspacePreferencesRequest;
import com.acme.financeportal.v1.GetFinanceWorkspacePreferencesResponse;
import com.acme.financeportal.v1.ListFinanceWorkspaceSubscriptionsRequest;
import com.acme.financeportal.v1.ListFinanceWorkspaceSubscriptionsResponse;
import com.acme.financeportal.v1.NotifyFinanceWorkspaceWatchersRequest;
import com.acme.financeportal.v1.NotifyFinanceWorkspaceWatchersResponse;
import com.acme.financeportal.v1.RelayApprovalInboxMessagesRequest;
import com.acme.financeportal.v1.RelayApprovalInboxMessagesResponse;
import com.acme.financeportal.v1.ReplayFinanceWorkspaceEventsRequest;
import com.acme.financeportal.v1.ReplayFinanceWorkspaceEventsResponse;
import com.acme.financeportal.v1.SendFinanceWorkspaceTestNoticeRequest;
import com.acme.financeportal.v1.SendFinanceWorkspaceTestNoticeResponse;
import com.acme.financeportal.v1.StreamFinanceWorkspaceNoticesRequest;
import com.acme.financeportal.v1.StreamFinanceWorkspaceNoticesResponse;
import com.acme.financeportal.v1.SubscribeFinanceWorkspaceUpdatesRequest;
import com.acme.financeportal.v1.SubscribeFinanceWorkspaceUpdatesResponse;
import com.acme.financeportal.v1.UnsubscribeFinanceWorkspaceUpdatesRequest;
import com.acme.financeportal.v1.UnsubscribeFinanceWorkspaceUpdatesResponse;
import com.acme.identity.v1.ExplainUserDecisionRequest;
import com.acme.identity.v1.ExplainUserDecisionResponse;
import com.acme.identity.v1.NormalizeUserRequest;
import com.acme.identity.v1.NormalizeUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserValidationServiceGrpc;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.FiscalPeriodHistoryServiceGrpc;
import com.acme.ledger.v1.FiscalPeriodParticipant;
import com.acme.ledger.v1.RevertFiscalPeriodRequest;
import com.acme.ledger.v1.RevertFiscalPeriodResponse;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionWorkflowServiceGrpc;
import com.acme.treasury.v1.ResumeCashPositionWorkflowRequest;
import com.acme.treasury.v1.ResumeCashPositionWorkflowResponse;
import com.google.protobuf.Any;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class FinanceWorkspaceNotificationApplicationTest {
    @Test
    void notifyFinanceWorkspaceWatchersPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new FinanceportalRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveApprovalInbox(String tenant, ApprovalInbox value) {}
            public void saveCloseChecklist(String tenant, CloseChecklist value) {}
            public void saveFinanceWorkspace(String tenant, FinanceWorkspace value) {}
        };
        var listener = new FinanceWorkspaceNotificationApplicationEvents(FanSessionQueryServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof FinanceWorkspaceNotificationApplication.ListFinanceWorkspaceSubscriptionsStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new FinanceWorkspaceNotificationApplication(
                AuditTrailHistoryServiceGrpc.newBlockingStub(channel),
                RetentionPolicyHistoryServiceGrpc.newBlockingStub(channel),
                RetentionPolicyValidationServiceGrpc.newBlockingStub(channel),
                CreditNoteWorkflowServiceGrpc.newBlockingStub(channel),
                VarianceImportServiceGrpc.newStub(channel),
                HouseholdApprovalServiceGrpc.newBlockingStub(channel),
                RouteBindingServiceGrpc.newBlockingStub(channel),
                events,
                ApprovalInboxIntegrationServiceGrpc.newStub(channel),
                FinanceWorkspaceIntegrationServiceGrpc.newBlockingStub(channel),
                UserValidationServiceGrpc.newBlockingStub(channel),
                FiscalPeriodHistoryServiceGrpc.newBlockingStub(channel),
                new FinanceWorkspaceNotificationRequestMapperImpl(),
                repository,
                CashPositionWorkflowServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(NotifyFinanceWorkspaceWatchersRequest.getDefaultInstance());
        var response = application.notifyFinanceWorkspaceWatchers(request);
        var sent0 = channel.request("acme.financeportal.v1.FinanceWorkspaceIntegrationService/AcknowledgeFinanceWorkspaceEvent");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent0, "reason"));
        var sent1 = channel.request("acme.identity.v1.UserValidationService/NormalizeUser");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.FinanceWorkspaceIntegrationService/AcknowledgeFinanceWorkspaceEvent"), "finance_workspace", "requester", "phone"),
                ProbeSupport.value(sent1, "details", "contact", "phone"));
        var sent2 = channel.request("acme.customers.v1.HouseholdApprovalService/ApproveHousehold");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.identity.v1.UserValidationService/NormalizeUser"), "user", "relationship_manager", "user_id"),
                ProbeSupport.value(sent2, "household", "children", "children", "manager", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.FinanceWorkspaceIntegrationService/AcknowledgeFinanceWorkspaceEvent"), "finance_workspace", "requester", "email"),
                ProbeSupport.value(sent2, "household", "children", "children", "manager", "email"));
        var sent3 = channel.request("acme.ledger.v1.FiscalPeriodHistoryService/RevertFiscalPeriod");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.identity.v1.UserValidationService/NormalizeUser"), "user", "contact", "preferred_channel"),
                ProbeSupport.value(sent3, "fiscal_period", "participants", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.identity.v1.UserValidationService/NormalizeUser"), "user", "contact", "phone"),
                ProbeSupport.value(sent3, "fiscal_period", "participants", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "recipient", "name"),
                ProbeSupport.value(sent3, "fiscal_period", "participants", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.FinanceWorkspaceIntegrationService/AcknowledgeFinanceWorkspaceEvent"), "finance_workspace", "customer", "email"),
                ProbeSupport.value(sent3, "fiscal_period", "participants", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.FinanceWorkspaceIntegrationService/AcknowledgeFinanceWorkspaceEvent"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "finance_workspace_id"),
                ProbeSupport.value(response, "finance_workspace", "finance_workspace_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "recipient", "name"),
                ProbeSupport.value(response, "finance_workspace", "requester", "name"));
    }

    @Test
    void streamFinanceWorkspaceNoticesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new FinanceportalRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveApprovalInbox(String tenant, ApprovalInbox value) {}
            public void saveCloseChecklist(String tenant, CloseChecklist value) {}
            public void saveFinanceWorkspace(String tenant, FinanceWorkspace value) {}
        };
        var listener = new FinanceWorkspaceNotificationApplicationEvents(FanSessionQueryServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof FinanceWorkspaceNotificationApplication.ListFinanceWorkspaceSubscriptionsStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new FinanceWorkspaceNotificationApplication(
                AuditTrailHistoryServiceGrpc.newBlockingStub(channel),
                RetentionPolicyHistoryServiceGrpc.newBlockingStub(channel),
                RetentionPolicyValidationServiceGrpc.newBlockingStub(channel),
                CreditNoteWorkflowServiceGrpc.newBlockingStub(channel),
                VarianceImportServiceGrpc.newStub(channel),
                HouseholdApprovalServiceGrpc.newBlockingStub(channel),
                RouteBindingServiceGrpc.newBlockingStub(channel),
                events,
                ApprovalInboxIntegrationServiceGrpc.newStub(channel),
                FinanceWorkspaceIntegrationServiceGrpc.newBlockingStub(channel),
                UserValidationServiceGrpc.newBlockingStub(channel),
                FiscalPeriodHistoryServiceGrpc.newBlockingStub(channel),
                new FinanceWorkspaceNotificationRequestMapperImpl(),
                repository,
                CashPositionWorkflowServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(StreamFinanceWorkspaceNoticesRequest.getDefaultInstance());
        var response = application.streamFinanceWorkspaceNotices(request);
        var sent0 = channel.request("acme.edgegateway.v1.RouteBindingService/CloneRouteBinding");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(sent0, "route_binding", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "user_id"),
                ProbeSupport.value(sent0, "route_binding", "owner", "user_id"));
        var sent1 = channel.request("acme.treasury.v1.CashPositionWorkflowService/ResumeCashPositionWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.edgegateway.v1.RouteBindingService/CloneRouteBinding"), "route_binding", "audit", "updated_at"),
                ProbeSupport.value(sent1, "cash_position", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.edgegateway.v1.RouteBindingService/CloneRouteBinding"), "route_binding", "audit", "etag"),
                ProbeSupport.value(sent1, "cash_position", "audit", "etag"));
        var sent2 = channel.request("acme.audit.v1.AuditTrailHistoryService/ListAuditTrailVersions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "labels"),
                ProbeSupport.value(sent2, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "query"),
                ProbeSupport.value(sent2, "filter", "query"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "created_after"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "owner_id"),
                ProbeSupport.value(sent2, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.edgegateway.v1.RouteBindingService/CloneRouteBinding"), "route_binding", "owner", "email"),
                ProbeSupport.value(response, "finance_workspace", "assignee", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionWorkflowService/ResumeCashPositionWorkflow"), "cash_position", "contact", "preferred_channel"),
                ProbeSupport.value(response, "finance_workspace", "requester", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailHistoryService/ListAuditTrailVersions"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailHistoryService/ListAuditTrailVersions"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(response, "finance_workspace", "requester", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "name"),
                ProbeSupport.value(response, "finance_workspace", "requester", "name"));
        var endpoint = new FinanceWorkspaceNotificationEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<StreamFinanceWorkspaceNoticesResponse> observer = new StreamObserver<>() {
            public void onNext(StreamFinanceWorkspaceNoticesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.streamFinanceWorkspaceNotices(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new FinanceWorkspaceNotificationApplicationEvents(FanSessionQueryServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<CountFanSessionsResponse>();
        listener.on(new FinanceWorkspaceNotificationApplication.ListFinanceWorkspaceSubscriptionsStage0Requested(ProbeSupport.sample(CountFanSessionsRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
