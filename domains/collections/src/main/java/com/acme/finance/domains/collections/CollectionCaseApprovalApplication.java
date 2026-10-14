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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Routes collection cases through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class CollectionCaseApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(CollectionCaseApprovalApplication.class);

    private final RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking;

    private final ChannelRegistry channels;

    private final CollectionCaseQueryServiceGrpc.CollectionCaseQueryServiceBlockingStub collectionsCollectionCaseQueryServiceBlocking;

    private final CollectionCaseServiceGrpc.CollectionCaseServiceBlockingStub collectionsCollectionCaseServiceBlocking;

    private final PaymentPromiseQueryServiceGrpc.PaymentPromiseQueryServiceBlockingStub collectionsPaymentPromiseQueryServiceBlocking;

    private final WriteOffValidationServiceGrpc.WriteOffValidationServiceBlockingStub collectionsWriteOffValidationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final FeatureFlagServiceGrpc.FeatureFlagServiceBlockingStub featureflagsFeatureFlagServiceBlocking;

    private final PolicyApprovalServiceGrpc.PolicyApprovalServiceBlockingStub insurancePolicyApprovalServiceBlocking;

    private final CollectionCaseApprovalRequestMapper mapper;

    private final NotificationQueryServiceGrpc.NotificationQueryServiceBlockingStub notificationsNotificationQueryServiceBlocking;

    private final NotificationValidationServiceGrpc.NotificationValidationServiceBlockingStub notificationsNotificationValidationServiceBlocking;

    private final CollectionsRepository repository;

    private final WebhookEndpointServiceGrpc.WebhookEndpointServiceBlockingStub webhooksWebhookEndpointServiceBlocking;

    public CollectionCaseApprovalApplication(
            RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking,
            ChannelRegistry channels,
            CollectionCaseQueryServiceGrpc.CollectionCaseQueryServiceBlockingStub collectionsCollectionCaseQueryServiceBlocking,
            CollectionCaseServiceGrpc.CollectionCaseServiceBlockingStub collectionsCollectionCaseServiceBlocking,
            PaymentPromiseQueryServiceGrpc.PaymentPromiseQueryServiceBlockingStub collectionsPaymentPromiseQueryServiceBlocking,
            WriteOffValidationServiceGrpc.WriteOffValidationServiceBlockingStub collectionsWriteOffValidationServiceBlocking,
            ApplicationEventPublisher events,
            FeatureFlagServiceGrpc.FeatureFlagServiceBlockingStub featureflagsFeatureFlagServiceBlocking,
            PolicyApprovalServiceGrpc.PolicyApprovalServiceBlockingStub insurancePolicyApprovalServiceBlocking,
            CollectionCaseApprovalRequestMapper mapper,
            NotificationQueryServiceGrpc.NotificationQueryServiceBlockingStub notificationsNotificationQueryServiceBlocking,
            NotificationValidationServiceGrpc.NotificationValidationServiceBlockingStub notificationsNotificationValidationServiceBlocking,
            CollectionsRepository repository,
            WebhookEndpointServiceGrpc.WebhookEndpointServiceBlockingStub webhooksWebhookEndpointServiceBlocking) {
        this.auditRetentionPolicyNotificationServiceBlocking = auditRetentionPolicyNotificationServiceBlocking;
        this.channels = channels;
        this.collectionsCollectionCaseQueryServiceBlocking = collectionsCollectionCaseQueryServiceBlocking;
        this.collectionsCollectionCaseServiceBlocking = collectionsCollectionCaseServiceBlocking;
        this.collectionsPaymentPromiseQueryServiceBlocking = collectionsPaymentPromiseQueryServiceBlocking;
        this.collectionsWriteOffValidationServiceBlocking = collectionsWriteOffValidationServiceBlocking;
        this.events = events;
        this.featureflagsFeatureFlagServiceBlocking = featureflagsFeatureFlagServiceBlocking;
        this.insurancePolicyApprovalServiceBlocking = insurancePolicyApprovalServiceBlocking;
        this.mapper = mapper;
        this.notificationsNotificationQueryServiceBlocking = notificationsNotificationQueryServiceBlocking;
        this.notificationsNotificationValidationServiceBlocking = notificationsNotificationValidationServiceBlocking;
        this.repository = repository;
        this.webhooksWebhookEndpointServiceBlocking = webhooksWebhookEndpointServiceBlocking;
    }

    public record ApproveCollectionCaseStage0Requested(SubscribeRetentionPolicyUpdatesRequest request, CompletableFuture<SubscribeRetentionPolicyUpdatesResponse> completion, io.grpc.Context context) {}

    /**
     * Submits a collection case.
     */
    public SubmitCollectionCaseResponse submitCollectionCase(SubmitCollectionCaseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitCollectionCase")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollectionCase(request.getContext().getTenantId(), request.getCollectionCase());

        var outbound0 = ResolveNotificationsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        final class Stage0Adapter {
            ResolveNotificationsResponse fetch(ResolveNotificationsRequest input) {
                return notificationsNotificationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveNotifications(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getCollectionCase().getCollectionCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitCollectionCase", request.getCollectionCase().getCollectionCaseId());
        return SubmitCollectionCaseResponse.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setCollectionCaseId(request.getCollectionCaseId())
                        .setStatus(request.getCollectionCase().getStatus())
                        .build())
                .setOutcome(SubmitCollectionCaseResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Approves a collection case.
     */
    public ApproveCollectionCaseResponse approveCollectionCase(ApproveCollectionCaseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveCollectionCase")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollectionCase(request.getContext().getTenantId(), request.getCollectionCase());

        var outbound0 = SubscribeRetentionPolicyUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubscribeRetentionPolicyUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getCollectionCase().getAssignee().getEmail())
                                .setName(request.getCollectionCase().getRequester().getName())
                                .setPreferredChannel(request.getCollectionCase().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<SubscribeRetentionPolicyUpdatesResponse>();
        events.publishEvent(new ApproveCollectionCaseStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        List<EscalatePolicyResponse> results1 = request.getCollectionCase().getCommentsList().stream()
                .map(element -> EscalatePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EscalatePolicyRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .setComment(element.getCommentId())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(element.getAuthorEmail())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(insurancePolicyApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalatePolicy(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? EscalatePolicyResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = ResolvePaymentPromisesRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        RpcPort<ResolvePaymentPromisesRequest, ResolvePaymentPromisesResponse> port2 = RpcPort.adapt(collectionsPaymentPromiseQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::resolvePaymentPromises);
        var reply2 = port2.exchange(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getCollectionCase().getCollectionCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveCollectionCase", request.getCollectionCase().getCollectionCaseId());
        return ApproveCollectionCaseResponse.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getRetentionPolicy().getAudit().getEtag())
                                .build())
                        .setCollectionCaseId(request.getCollectionCaseId())
                        .setPriority(request.getCollectionCase().getPriority())
                        .build())
                .setOutcome(ApproveCollectionCaseResponse.Outcome.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Rejects a collection case.
     */
    public RejectCollectionCaseResponse rejectCollectionCase(RejectCollectionCaseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectCollectionCase")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollectionCase(request.getContext().getTenantId(), request.getCollectionCase());

        var outbound0 = mapper.rejectCollectionCaseStage0(request);
        var reply0 = webhooksWebhookEndpointServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchWebhookEndpoints(outbound0);

        var outbound1 = JsonCodec.roundTrip(CloneCollectionCaseRequest.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getCollectionCase().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build(), CloneCollectionCaseRequest.class);
        var reply1 = collectionsCollectionCaseServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneCollectionCase(outbound1);

        var outbound2 = SubscribeRetentionPolicyUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubscribeRetentionPolicyUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(reply1.getCollectionCase().getRequester().getEmail())
                                .setPhone(reply1.getCollectionCase().getRequester().getPhone())
                                .setPreferredChannel(reply1.getCollectionCase().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var adapter2 = new Function<SubscribeRetentionPolicyUpdatesRequest, SubscribeRetentionPolicyUpdatesResponse>() {
            @Override
            public SubscribeRetentionPolicyUpdatesResponse apply(SubscribeRetentionPolicyUpdatesRequest input) {
                return auditRetentionPolicyNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).subscribeRetentionPolicyUpdates(input);
            }
        };
        var reply2 = adapter2.apply(outbound2);

        var outbound3 = (CheckNotificationEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setNotification(Notification.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply2.getRetentionPolicy().getAudit().getEtag())
                                .build())
                        .setChannel(NotificationChannelOrigin.newBuilder()
                                .setAddress(request.getCollectionCase().getChannel().getAddress())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply3 = notificationsNotificationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkNotificationEligibility(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getCollectionCase().getSubject()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectCollectionCase", request.getCollectionCase().getSubject());
        return RejectCollectionCaseResponse.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setAssignee(request.getCollectionCase().getAssignee())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getCollectionCase().getAudit().getCreatedAt())
                                .build())
                        .setCollectionCaseId(request.getCollectionCaseId())
                        .build())
                .setOutcome(RejectCollectionCaseResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply3.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Escalates a collection case.
     */
    public EscalateCollectionCaseResponse escalateCollectionCase(EscalateCollectionCaseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateCollectionCase")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollectionCase(request.getContext().getTenantId(), request.getCollectionCase());

        var builder0 = ValidateWriteOffRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDetails(ValidateWriteOffRequest.Details.newBuilder()
                .setContact(ContactInfo.newBuilder()
                        .setEmail(request.getCollectionCase().getRequester().getEmail())
                        .setName(request.getCollectionCase().getRequester().getName())
                        .build())
                .build());
        builder0.setWriteOff(WriteOff.newBuilder()
                .setCounterparty(CollectionCaseRef.newBuilder()
                        .setPriority(request.getCollectionCase().getPriority())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = collectionsWriteOffValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateWriteOff(outbound0);

        var outbound1 = UpdateWebhookEndpointRequest.newBuilder()
                .setContext(request.getContext())
                .setWebhookEndpoint(WebhookEndpoint.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getWriteOff().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.webhooks.v1.WebhookEndpointService");
        var serviceName1 = "acme.webhooks.v1" + "." + "WebhookEndpointService";
        var methodName1 = "Upd" + "ateWebhookEndpoint";
        var descriptor1 = MethodDescriptor.<UpdateWebhookEndpointRequest, UpdateWebhookEndpointResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(UpdateWebhookEndpointRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(UpdateWebhookEndpointResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);

        var outbound2 = mapper.escalateCollectionCaseStage2(request, reply1);
        var reply2 = collectionsCollectionCaseServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).deleteCollectionCase(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getCollectionCase().getSubject()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateCollectionCase", request.getCollectionCase().getSubject());
        return EscalateCollectionCaseResponse.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply1.getWebhookEndpoint().getOwner().getDisplayName())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getWriteOff().getAudit().getCreatedBy())
                                .build())
                        .setCollectionCaseId(request.getCollectionCaseId())
                        .putAllCustomFields(request.getCollectionCase().getCustomFieldsMap())
                        .build())
                .setOutcome(EscalateCollectionCaseResponse.Outcome.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .setReferenceId(reply0.getOutcome().getReferenceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Lists collection cases a page at a time.
     */
    public ListPendingCollectionCasesResponse listPendingCollectionCases(ListPendingCollectionCasesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingCollectionCases")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CollectionCase", request.getContext().getTraceId(), "ListPendingCollectionCases");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingCollectionCases", request.getPage().getPageToken());
        return ListPendingCollectionCasesResponse.getDefaultInstance();
    }

    /**
     * Returns a collection case.
     */
    public GetCollectionCaseApprovalStatusResponse getCollectionCaseApprovalStatus(GetCollectionCaseApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetCollectionCaseApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CollectionCase", request.getCollectionCaseId(), "GetCollectionCaseApprovalStatus");

        var outbound0 = PeekCollectionCaseRequest.newBuilder()
                .setCollectionCaseId(request.getCollectionCaseId())
                .setContext(request.getContext())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> collectionsCollectionCaseQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).peekCollectionCase(value));

        var outbound1 = RpcSupport.unpack(Any.pack(CloneFeatureFlagRequest.newBuilder()
                .setContext(request.getContext())
                .setFeatureFlag(FeatureFlag.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getCollectionCase().getAudit().getCreatedAt())
                                .setCreatedBy(reply0.getCollectionCase().getAudit().getCreatedBy())
                                .setUpdatedAt(reply0.getCollectionCase().getAudit().getUpdatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getCollectionCase().getAssignee().getEmail())
                                .build())
                        .build())
                .build()), CloneFeatureFlagRequest.class);
        var reply1 = featureflagsFeatureFlagServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneFeatureFlag(outbound1);
        return GetCollectionCaseApprovalStatusResponse.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply1.getFeatureFlag().getOwner().getDisplayName())
                                .setUserId(reply0.getCollectionCase().getAssignee().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getFeatureFlag().getAudit().getUpdatedBy())
                                .build())
                        .setCollectionCaseId(request.getCollectionCaseId())
                        .build())
                .build();
    }
}
