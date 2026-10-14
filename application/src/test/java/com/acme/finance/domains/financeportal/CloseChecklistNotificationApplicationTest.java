package com.acme.finance.domains.financeportal;

import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.FindAuditEventsRequest;
import com.acme.audit.v1.FindAuditEventsResponse;
import com.acme.audit.v1.SubscribeAuditEventUpdatesRequest;
import com.acme.audit.v1.SubscribeAuditEventUpdatesResponse;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyBlockAction;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.AccessPolicyValidationServiceGrpc;
import com.acme.authorization.v1.CreateAccessPolicyRequest;
import com.acme.authorization.v1.CreateAccessPolicyResponse;
import com.acme.authorization.v1.PreviewAccessPolicyRequest;
import com.acme.authorization.v1.PreviewAccessPolicyResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CommentConsentRecordApprovalRequest;
import com.acme.customers.v1.CommentConsentRecordApprovalResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordApprovalServiceGrpc;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerLifecycleServiceGrpc;
import com.acme.customers.v1.CustomerPassport;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentBlockAction;
import com.acme.customers.v1.CustomerSegmentSyncServiceGrpc;
import com.acme.customers.v1.CustomerWorkflowServiceGrpc;
import com.acme.customers.v1.PauseCustomerWorkflowRequest;
import com.acme.customers.v1.PauseCustomerWorkflowResponse;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceImportServiceGrpc;
import com.acme.customers.v1.PushCustomerSegmentRequest;
import com.acme.customers.v1.PushCustomerSegmentResponse;
import com.acme.customers.v1.ScheduleCustomerExpiryRequest;
import com.acme.customers.v1.ScheduleCustomerExpiryResponse;
import com.acme.customers.v1.ValidatePreferenceImportRequest;
import com.acme.customers.v1.ValidatePreferenceImportResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.financeportal.v1.AcknowledgeFinanceWorkspaceEventRequest;
import com.acme.financeportal.v1.AcknowledgeFinanceWorkspaceEventResponse;
import com.acme.financeportal.v1.ApprovalInbox;
import com.acme.financeportal.v1.CloseChecklist;
import com.acme.financeportal.v1.DisconnectFinanceWorkspaceSourceRequest;
import com.acme.financeportal.v1.DisconnectFinanceWorkspaceSourceResponse;
import com.acme.financeportal.v1.FinanceWorkspace;
import com.acme.financeportal.v1.FinanceWorkspaceIntegrationServiceGrpc;
import com.acme.financeportal.v1.FinanceWorkspaceNotificationServiceGrpc;
import com.acme.financeportal.v1.GetCloseChecklistPreferencesRequest;
import com.acme.financeportal.v1.GetCloseChecklistPreferencesResponse;
import com.acme.financeportal.v1.ListCloseChecklistSubscriptionsRequest;
import com.acme.financeportal.v1.ListCloseChecklistSubscriptionsResponse;
import com.acme.financeportal.v1.ListFinanceWorkspaceSourcesRequest;
import com.acme.financeportal.v1.ListFinanceWorkspaceSourcesResponse;
import com.acme.financeportal.v1.NotifyCloseChecklistWatchersRequest;
import com.acme.financeportal.v1.NotifyCloseChecklistWatchersResponse;
import com.acme.financeportal.v1.SendCloseChecklistTestNoticeRequest;
import com.acme.financeportal.v1.SendCloseChecklistTestNoticeResponse;
import com.acme.financeportal.v1.StreamCloseChecklistNoticesRequest;
import com.acme.financeportal.v1.StreamCloseChecklistNoticesResponse;
import com.acme.financeportal.v1.StreamFinanceWorkspaceNoticesRequest;
import com.acme.financeportal.v1.StreamFinanceWorkspaceNoticesResponse;
import com.acme.financeportal.v1.SubscribeCloseChecklistUpdatesRequest;
import com.acme.financeportal.v1.SubscribeCloseChecklistUpdatesResponse;
import com.acme.financeportal.v1.SubscribeFinanceWorkspaceUpdatesRequest;
import com.acme.financeportal.v1.SubscribeFinanceWorkspaceUpdatesResponse;
import com.acme.financeportal.v1.UnsubscribeCloseChecklistUpdatesRequest;
import com.acme.financeportal.v1.UnsubscribeCloseChecklistUpdatesResponse;
import com.acme.identity.v1.UserRef;
import com.acme.suppliers.v1.ApproveSupplierRatingRequest;
import com.acme.suppliers.v1.ApproveSupplierRatingResponse;
import com.acme.suppliers.v1.ListSupplierContractWorkflowsRequest;
import com.acme.suppliers.v1.ListSupplierContractWorkflowsResponse;
import com.acme.suppliers.v1.SupplierContractWorkflowServiceGrpc;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingApprovalServiceGrpc;
import com.acme.suppliers.v1.SupplierRatingLine;
import com.acme.suppliers.v1.SupplierRatingLineTax;
import com.acme.treasury.v1.CommentLiquidityForecastApprovalRequest;
import com.acme.treasury.v1.CommentLiquidityForecastApprovalResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.LiquidityForecastApprovalServiceGrpc;
import com.acme.venues.v1.SearchVenuesRequest;
import com.acme.venues.v1.SearchVenuesResponse;
import com.acme.venues.v1.VenueServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class CloseChecklistNotificationApplicationTest {
    @Test
    void streamCloseChecklistNoticesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new FinanceportalRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveApprovalInbox(String tenant, ApprovalInbox value) {}
            public void saveCloseChecklist(String tenant, CloseChecklist value) {}
            public void saveFinanceWorkspace(String tenant, FinanceWorkspace value) {}
        };
        var application = new CloseChecklistNotificationApplication(
                AuditEventNotificationServiceGrpc.newBlockingStub(channel),
                AuditEventQueryServiceGrpc.newBlockingStub(channel),
                AccessPolicyServiceGrpc.newBlockingStub(channel),
                AccessPolicyValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ConsentRecordApprovalServiceGrpc.newBlockingStub(channel),
                CustomerLifecycleServiceGrpc.newBlockingStub(channel),
                CustomerSegmentSyncServiceGrpc.newBlockingStub(channel),
                CustomerWorkflowServiceGrpc.newBlockingStub(channel),
                PreferenceImportServiceGrpc.newBlockingStub(channel),
                FinanceWorkspaceIntegrationServiceGrpc.newBlockingStub(channel),
                FinanceWorkspaceNotificationServiceGrpc.newBlockingStub(channel),
                repository,
                SupplierContractWorkflowServiceGrpc.newFutureStub(channel),
                SupplierRatingApprovalServiceGrpc.newBlockingStub(channel),
                LiquidityForecastApprovalServiceGrpc.newBlockingStub(channel),
                VenueServiceGrpc.newFutureStub(channel));
        var request = ProbeSupport.sample(StreamCloseChecklistNoticesRequest.getDefaultInstance());
        var response = application.streamCloseChecklistNotices(request);
        var sent0 = channel.request("acme.financeportal.v1.FinanceWorkspaceNotificationService/StreamFinanceWorkspaceNotices");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "labels"),
                ProbeSupport.value(sent0, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "preferred_channel"),
                ProbeSupport.value(sent0, "subscriber", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "phone"),
                ProbeSupport.value(sent0, "subscriber", "phone"));
        var sent1 = channel.request("acme.financeportal.v1.FinanceWorkspaceIntegrationService/AcknowledgeFinanceWorkspaceEvent");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "since"),
                ProbeSupport.value(sent1, "details", "received_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "owner_id"),
                ProbeSupport.value(sent1, "details", "message_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "labels"),
                ProbeSupport.value(sent1, "annotations"));
        var sent2 = channel.request("acme.audit.v1.AuditEventQueryService/FindAuditEvents");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "labels"),
                ProbeSupport.value(sent2, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "query"),
                ProbeSupport.value(sent2, "filter", "query"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "created_after"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        var sent3 = channel.request("acme.authorization.v1.AccessPolicyService/CreateAccessPolicy");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.FinanceWorkspaceIntegrationService/AcknowledgeFinanceWorkspaceEvent"), "finance_workspace", "audit", "created_at"),
                ProbeSupport.value(sent3, "access_policy", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.FinanceWorkspaceIntegrationService/AcknowledgeFinanceWorkspaceEvent"), "finance_workspace", "audit", "etag"),
                ProbeSupport.value(sent3, "access_policy", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.FinanceWorkspaceIntegrationService/AcknowledgeFinanceWorkspaceEvent"), "finance_workspace", "assignee", "user_id"),
                ProbeSupport.value(sent3, "access_policy", "owner", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.FinanceWorkspaceIntegrationService/AcknowledgeFinanceWorkspaceEvent"), "finance_workspace", "requester", "email"),
                ProbeSupport.value(response, "close_checklist", "counterparty", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.FinanceWorkspaceIntegrationService/AcknowledgeFinanceWorkspaceEvent"), "outcome", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventQueryService/FindAuditEvents"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.authorization.v1.AccessPolicyService/CreateAccessPolicy"), "access_policy", "audit", "etag"),
                ProbeSupport.value(response, "close_checklist", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(response, "close_checklist", "billing_address", "region"));
        var endpoint = new CloseChecklistNotificationEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<StreamCloseChecklistNoticesResponse> observer = new StreamObserver<>() {
            public void onNext(StreamCloseChecklistNoticesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.streamCloseChecklistNotices(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }
}
