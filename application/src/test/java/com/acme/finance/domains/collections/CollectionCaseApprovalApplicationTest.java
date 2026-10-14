package com.acme.finance.domains.collections;

import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.SubscribeRetentionPolicyUpdatesRequest;
import com.acme.audit.v1.SubscribeRetentionPolicyUpdatesResponse;
import com.acme.collections.v1.ApproveCollectionCaseRequest;
import com.acme.collections.v1.ApproveCollectionCaseResponse;
import com.acme.collections.v1.CloneCollectionCaseRequest;
import com.acme.collections.v1.CloneCollectionCaseResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseQueryServiceGrpc;
import com.acme.collections.v1.CollectionCaseRef;
import com.acme.collections.v1.CollectionCaseServiceGrpc;
import com.acme.collections.v1.DeleteCollectionCaseRequest;
import com.acme.collections.v1.DeleteCollectionCaseResponse;
import com.acme.collections.v1.EscalateCollectionCaseRequest;
import com.acme.collections.v1.EscalateCollectionCaseResponse;
import com.acme.collections.v1.GetCollectionCaseApprovalStatusRequest;
import com.acme.collections.v1.GetCollectionCaseApprovalStatusResponse;
import com.acme.collections.v1.ListPendingCollectionCasesRequest;
import com.acme.collections.v1.ListPendingCollectionCasesResponse;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PaymentPromiseQueryServiceGrpc;
import com.acme.collections.v1.PeekCollectionCaseRequest;
import com.acme.collections.v1.PeekCollectionCaseResponse;
import com.acme.collections.v1.RejectCollectionCaseRequest;
import com.acme.collections.v1.RejectCollectionCaseResponse;
import com.acme.collections.v1.ResolvePaymentPromisesRequest;
import com.acme.collections.v1.ResolvePaymentPromisesResponse;
import com.acme.collections.v1.SubmitCollectionCaseRequest;
import com.acme.collections.v1.SubmitCollectionCaseResponse;
import com.acme.collections.v1.ValidateWriteOffRequest;
import com.acme.collections.v1.ValidateWriteOffResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.featureflags.v1.CloneFeatureFlagRequest;
import com.acme.featureflags.v1.CloneFeatureFlagResponse;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.EscalatePolicyRequest;
import com.acme.insurance.v1.EscalatePolicyResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyApprovalServiceGrpc;
import com.acme.notifications.v1.CheckNotificationEligibilityRequest;
import com.acme.notifications.v1.CheckNotificationEligibilityResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationChannelOrigin;
import com.acme.notifications.v1.NotificationQueryServiceGrpc;
import com.acme.notifications.v1.NotificationValidationServiceGrpc;
import com.acme.notifications.v1.ResolveNotificationsRequest;
import com.acme.notifications.v1.ResolveNotificationsResponse;
import com.acme.webhooks.v1.SearchWebhookEndpointsRequest;
import com.acme.webhooks.v1.SearchWebhookEndpointsResponse;
import com.acme.webhooks.v1.UpdateWebhookEndpointRequest;
import com.acme.webhooks.v1.UpdateWebhookEndpointResponse;
import com.acme.webhooks.v1.WebhookEndpoint;
import com.acme.webhooks.v1.WebhookEndpointServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.List;
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

class CollectionCaseApprovalApplicationTest {
    @Test
    void rejectCollectionCasePreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new CollectionsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveCollectionCase(String tenant, CollectionCase value) {}
            public void savePaymentPromise(String tenant, PaymentPromise value) {}
            public void saveWriteOff(String tenant, WriteOff value) {}
        };
        var listener = new CollectionCaseApprovalApplicationEvents(RetentionPolicyNotificationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof CollectionCaseApprovalApplication.ApproveCollectionCaseStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new CollectionCaseApprovalApplication(
                RetentionPolicyNotificationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CollectionCaseQueryServiceGrpc.newBlockingStub(channel),
                CollectionCaseServiceGrpc.newBlockingStub(channel),
                PaymentPromiseQueryServiceGrpc.newBlockingStub(channel),
                WriteOffValidationServiceGrpc.newBlockingStub(channel),
                events,
                FeatureFlagServiceGrpc.newBlockingStub(channel),
                PolicyApprovalServiceGrpc.newBlockingStub(channel),
                new CollectionCaseApprovalRequestMapperImpl(),
                NotificationQueryServiceGrpc.newBlockingStub(channel),
                NotificationValidationServiceGrpc.newBlockingStub(channel),
                repository,
                WebhookEndpointServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(RejectCollectionCaseRequest.getDefaultInstance());
        var response = application.rejectCollectionCase(request);
        var sent0 = channel.request("acme.webhooks.v1.WebhookEndpointService/SearchWebhookEndpoints");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "collection_case", "audit", "created_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "collection_case", "collection_case_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "filter", "labels"));
        var sent1 = channel.request("acme.collections.v1.CollectionCaseService/CloneCollectionCase");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "collection_case", "audit", "updated_at"),
                ProbeSupport.value(sent1, "collection_case", "audit", "updated_at"));
        var sent2 = channel.request("acme.audit.v1.RetentionPolicyNotificationService/SubscribeRetentionPolicyUpdates");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.CollectionCaseService/CloneCollectionCase"), "collection_case", "requester", "phone"),
                ProbeSupport.value(sent2, "details", "recipient", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.CollectionCaseService/CloneCollectionCase"), "collection_case", "requester", "preferred_channel"),
                ProbeSupport.value(sent2, "details", "recipient", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.CollectionCaseService/CloneCollectionCase"), "collection_case", "requester", "email"),
                ProbeSupport.value(sent2, "details", "recipient", "email"));
        var sent3 = channel.request("acme.notifications.v1.NotificationValidationService/CheckNotificationEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "collection_case", "channel", "address"),
                ProbeSupport.value(sent3, "notification", "channel", "address"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyNotificationService/SubscribeRetentionPolicyUpdates"), "retention_policy", "audit", "etag"),
                ProbeSupport.value(sent3, "notification", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.webhooks.v1.WebhookEndpointService/SearchWebhookEndpoints"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.CollectionCaseService/CloneCollectionCase"), "collection_case", "audit", "created_at"),
                ProbeSupport.value(response, "collection_case", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.CollectionCaseService/CloneCollectionCase"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyNotificationService/SubscribeRetentionPolicyUpdates"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationValidationService/CheckNotificationEligibility"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "collection_case_id"),
                ProbeSupport.value(response, "collection_case", "collection_case_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "collection_case", "assignee"),
                ProbeSupport.value(response, "collection_case", "assignee"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new CollectionCaseApprovalApplicationEvents(RetentionPolicyNotificationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<SubscribeRetentionPolicyUpdatesResponse>();
        listener.on(new CollectionCaseApprovalApplication.ApproveCollectionCaseStage0Requested(ProbeSupport.sample(SubscribeRetentionPolicyUpdatesRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
