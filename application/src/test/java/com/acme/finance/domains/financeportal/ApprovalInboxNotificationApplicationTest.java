package com.acme.finance.domains.financeportal;

import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.audit.v1.LintAuditTrailsRequest;
import com.acme.audit.v1.LintAuditTrailsResponse;
import com.acme.audit.v1.ListAuditEventSubscriptionsRequest;
import com.acme.audit.v1.ListAuditEventSubscriptionsResponse;
import com.acme.audit.v1.RecordRetentionPolicyVersionRequest;
import com.acme.audit.v1.RecordRetentionPolicyVersionResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.financeportal.v1.AcknowledgeCloseChecklistEventRequest;
import com.acme.financeportal.v1.AcknowledgeCloseChecklistEventResponse;
import com.acme.financeportal.v1.ApprovalInbox;
import com.acme.financeportal.v1.ApprovalInboxIntegrationServiceGrpc;
import com.acme.financeportal.v1.ApprovalInboxWorkflowServiceGrpc;
import com.acme.financeportal.v1.CloseChecklist;
import com.acme.financeportal.v1.CloseChecklistCardSettlement;
import com.acme.financeportal.v1.CloseChecklistIntegrationServiceGrpc;
import com.acme.financeportal.v1.CloseChecklistRef;
import com.acme.financeportal.v1.ConnectCloseChecklistSourceRequest;
import com.acme.financeportal.v1.ConnectCloseChecklistSourceResponse;
import com.acme.financeportal.v1.DisconnectApprovalInboxSourceRequest;
import com.acme.financeportal.v1.DisconnectApprovalInboxSourceResponse;
import com.acme.financeportal.v1.FinanceWorkspace;
import com.acme.financeportal.v1.GetApprovalInboxPreferencesRequest;
import com.acme.financeportal.v1.GetApprovalInboxPreferencesResponse;
import com.acme.financeportal.v1.ListApprovalInboxSubscriptionsRequest;
import com.acme.financeportal.v1.ListApprovalInboxSubscriptionsResponse;
import com.acme.financeportal.v1.ListCloseChecklistSourcesRequest;
import com.acme.financeportal.v1.ListCloseChecklistSourcesResponse;
import com.acme.financeportal.v1.NotifyApprovalInboxWatchersRequest;
import com.acme.financeportal.v1.NotifyApprovalInboxWatchersResponse;
import com.acme.financeportal.v1.RelayCloseChecklistMessagesRequest;
import com.acme.financeportal.v1.RelayCloseChecklistMessagesResponse;
import com.acme.financeportal.v1.ReplayCloseChecklistEventsRequest;
import com.acme.financeportal.v1.ReplayCloseChecklistEventsResponse;
import com.acme.financeportal.v1.SendApprovalInboxTestNoticeRequest;
import com.acme.financeportal.v1.SendApprovalInboxTestNoticeResponse;
import com.acme.financeportal.v1.SignalApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.SignalApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.StreamApprovalInboxNoticesRequest;
import com.acme.financeportal.v1.StreamApprovalInboxNoticesResponse;
import com.acme.financeportal.v1.SubscribeApprovalInboxUpdatesRequest;
import com.acme.financeportal.v1.SubscribeApprovalInboxUpdatesResponse;
import com.acme.financeportal.v1.TestApprovalInboxConnectionRequest;
import com.acme.financeportal.v1.TestApprovalInboxConnectionResponse;
import com.acme.financeportal.v1.TestCloseChecklistConnectionRequest;
import com.acme.financeportal.v1.TestCloseChecklistConnectionResponse;
import com.acme.financeportal.v1.UnsubscribeApprovalInboxUpdatesRequest;
import com.acme.financeportal.v1.UnsubscribeApprovalInboxUpdatesResponse;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CoverageWorkflowServiceGrpc;
import com.acme.insurance.v1.PauseCoverageWorkflowRequest;
import com.acme.insurance.v1.PauseCoverageWorkflowResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountCardFunding;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.ledger.v1.PostingBatchRef;
import com.acme.ledger.v1.RecordLedgerAccountVersionRequest;
import com.acme.ledger.v1.RecordLedgerAccountVersionResponse;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferLine;
import com.acme.treasury.v1.CommentFxHedgeApprovalRequest;
import com.acme.treasury.v1.CommentFxHedgeApprovalResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeApprovalServiceGrpc;
import com.acme.treasury.v1.SubmitBankTransferRequest;
import com.acme.treasury.v1.SubmitBankTransferResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class ApprovalInboxNotificationApplicationTest {
    @Test
    void streamApprovalInboxNoticesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new FinanceportalRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveApprovalInbox(String tenant, ApprovalInbox value) {}
            public void saveCloseChecklist(String tenant, CloseChecklist value) {}
            public void saveFinanceWorkspace(String tenant, FinanceWorkspace value) {}
        };
        var listener = new ApprovalInboxNotificationApplicationEvents(CloseChecklistIntegrationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof ApprovalInboxNotificationApplication.GetApprovalInboxPreferencesStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new ApprovalInboxNotificationApplication(
                AuditEventNotificationServiceGrpc.newBlockingStub(channel),
                AuditTrailValidationServiceGrpc.newBlockingStub(channel),
                RetentionPolicyHistoryServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                events,
                ApprovalInboxIntegrationServiceGrpc.newBlockingStub(channel),
                ApprovalInboxWorkflowServiceGrpc.newBlockingStub(channel),
                CloseChecklistIntegrationServiceGrpc.newStub(channel),
                CloseChecklistIntegrationServiceGrpc.newBlockingStub(channel),
                CoverageWorkflowServiceGrpc.newBlockingStub(channel),
                LedgerAccountHistoryServiceGrpc.newBlockingStub(channel),
                repository,
                FxHedgeApprovalServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(StreamApprovalInboxNoticesRequest.getDefaultInstance());
        var response = application.streamApprovalInboxNotices(request);
        var sent0 = channel.request("acme.treasury.v1.FxHedgeApprovalService/CommentFxHedgeApproval");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent0, "fx_hedge", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(sent0, "details", "approver", "email"));
        var sent1 = channel.request("acme.financeportal.v1.CloseChecklistIntegrationService/RelayCloseChecklistMessages");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.FxHedgeApprovalService/CommentFxHedgeApproval"), "fx_hedge", "card", "token"),
                ProbeSupport.value(sent1, "change", "card", "token"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.FxHedgeApprovalService/CommentFxHedgeApproval"), "fx_hedge", "card", "last4"),
                ProbeSupport.value(sent1, "change", "card", "last4"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.FxHedgeApprovalService/CommentFxHedgeApproval"), "fx_hedge", "counterparty", "display_name"),
                ProbeSupport.value(sent1, "change", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.FxHedgeApprovalService/CommentFxHedgeApproval"), "fx_hedge", "audit", "updated_by"),
                ProbeSupport.value(sent1, "change", "audit", "updated_by"));
        var sent2 = channel.request("acme.ledger.v1.LedgerAccountHistoryService/RecordLedgerAccountVersion");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.FxHedgeApprovalService/CommentFxHedgeApproval"), "fx_hedge", "card", "holder_name"),
                ProbeSupport.value(sent2, "ledger_account", "card", "holder_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.FxHedgeApprovalService/CommentFxHedgeApproval"), "fx_hedge", "card", "network"),
                ProbeSupport.value(sent2, "ledger_account", "card", "network"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "phone"),
                ProbeSupport.value(sent2, "ledger_account", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.FxHedgeApprovalService/CommentFxHedgeApproval"), "fx_hedge", "counterparty", "display_name"),
                ProbeSupport.value(sent2, "ledger_account", "holder", "display_name"));
        var sent3 = channel.request("acme.financeportal.v1.CloseChecklistIntegrationService/ConnectCloseChecklistSource");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.FxHedgeApprovalService/CommentFxHedgeApproval"), "outcome", "message"),
                ProbeSupport.value(sent3, "details", "message_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountHistoryService/RecordLedgerAccountVersion"), "ledger_account", "audit", "updated_at"),
                ProbeSupport.value(sent3, "details", "received_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.FxHedgeApprovalService/CommentFxHedgeApproval"), "result", "details"),
                ProbeSupport.value(sent3, "annotations"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.FxHedgeApprovalService/CommentFxHedgeApproval"), "fx_hedge", "billing_address", "region"),
                ProbeSupport.value(response, "approval_inbox", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountHistoryService/RecordLedgerAccountVersion"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountHistoryService/RecordLedgerAccountVersion"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.CloseChecklistIntegrationService/ConnectCloseChecklistSource"), "close_checklist", "billing_address", "recipient"),
                ProbeSupport.value(response, "approval_inbox", "billing_address", "recipient"));
        var endpoint = new ApprovalInboxNotificationEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<StreamApprovalInboxNoticesResponse> observer = new StreamObserver<>() {
            public void onNext(StreamApprovalInboxNoticesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.streamApprovalInboxNotices(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new ApprovalInboxNotificationApplicationEvents(CloseChecklistIntegrationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<ListCloseChecklistSourcesResponse>();
        listener.on(new ApprovalInboxNotificationApplication.GetApprovalInboxPreferencesStage0Requested(ProbeSupport.sample(ListCloseChecklistSourcesRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
