package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailSyncServiceGrpc;
import com.acme.audit.v1.GetRetentionPolicyPreferencesRequest;
import com.acme.audit.v1.GetRetentionPolicyPreferencesResponse;
import com.acme.audit.v1.ListRetentionPolicyConflictsRequest;
import com.acme.audit.v1.ListRetentionPolicyConflictsResponse;
import com.acme.audit.v1.ListRetentionPolicySubscriptionsRequest;
import com.acme.audit.v1.ListRetentionPolicySubscriptionsResponse;
import com.acme.audit.v1.NotifyRetentionPolicyWatchersRequest;
import com.acme.audit.v1.NotifyRetentionPolicyWatchersResponse;
import com.acme.audit.v1.ResolveAuditTrailConflictRequest;
import com.acme.audit.v1.ResolveAuditTrailConflictResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyRef;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.SendRetentionPolicyTestNoticeRequest;
import com.acme.audit.v1.SendRetentionPolicyTestNoticeResponse;
import com.acme.audit.v1.StreamRetentionPolicyNoticesRequest;
import com.acme.audit.v1.StreamRetentionPolicyNoticesResponse;
import com.acme.audit.v1.SubscribeRetentionPolicyUpdatesRequest;
import com.acme.audit.v1.SubscribeRetentionPolicyUpdatesResponse;
import com.acme.audit.v1.UnsubscribeRetentionPolicyUpdatesRequest;
import com.acme.audit.v1.UnsubscribeRetentionPolicyUpdatesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.OperationResult;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Tells the people who follow a retention policy what changed. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class RetentionPolicyNotificationApplication {
    private static final Logger log = LoggerFactory.getLogger(RetentionPolicyNotificationApplication.class);

    private final AuditTrailSyncServiceGrpc.AuditTrailSyncServiceBlockingStub auditAuditTrailSyncServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceBlockingStub auditRetentionPolicySyncServiceBlocking;

    private final RetentionPolicyNotificationRequestMapper mapper;

    private final AuditRepository repository;

    public RetentionPolicyNotificationApplication(
            AuditTrailSyncServiceGrpc.AuditTrailSyncServiceBlockingStub auditAuditTrailSyncServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceBlockingStub auditRetentionPolicySyncServiceBlocking,
            RetentionPolicyNotificationRequestMapper mapper,
            AuditRepository repository) {
        this.auditAuditTrailSyncServiceBlocking = auditAuditTrailSyncServiceBlocking;
        this.auditRetentionPolicySyncServiceBlocking = auditRetentionPolicySyncServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
    }

    /**
     * Subscribes a retention policy.
     */
    public SubscribeRetentionPolicyUpdatesResponse subscribeRetentionPolicyUpdates(SubscribeRetentionPolicyUpdatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubscribeRetentionPolicyUpdates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RetentionPolicy", request.getRetentionPolicyId(), "SubscribeRetentionPolicyUpdates");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getPhone()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubscribeRetentionPolicyUpdates", request.getDetails().getRecipient().getPhone());
        return SubscribeRetentionPolicyUpdatesResponse.newBuilder()
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .build())
                        .setRetentionPolicyId(request.getRetentionPolicyId())
                        .build())
                .build();
    }

    /**
     * Unsubscribes a retention policy.
     */
    public UnsubscribeRetentionPolicyUpdatesResponse unsubscribeRetentionPolicyUpdates(UnsubscribeRetentionPolicyUpdatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "UnsubscribeRetentionPolicyUpdates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RetentionPolicy", request.getRetentionPolicyId(), "UnsubscribeRetentionPolicyUpdates");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getPhone()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "UnsubscribeRetentionPolicyUpdates", request.getDetails().getRecipient().getPhone());
        return UnsubscribeRetentionPolicyUpdatesResponse.newBuilder()
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .build())
                        .setRetentionPolicyId(request.getRetentionPolicyId())
                        .build())
                .build();
    }

    /**
     * Notifys a retention policy.
     */
    public NotifyRetentionPolicyWatchersResponse notifyRetentionPolicyWatchers(NotifyRetentionPolicyWatchersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NotifyRetentionPolicyWatchers")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RetentionPolicy", request.getRetentionPolicyId(), "NotifyRetentionPolicyWatchers");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getPhone()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NotifyRetentionPolicyWatchers", request.getDetails().getRecipient().getPhone());
        return NotifyRetentionPolicyWatchersResponse.newBuilder()
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .build())
                        .setRetentionPolicyId(request.getRetentionPolicyId())
                        .build())
                .build();
    }

    /**
     * Lists retention policies a page at a time.
     */
    public ListRetentionPolicySubscriptionsResponse listRetentionPolicySubscriptions(ListRetentionPolicySubscriptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListRetentionPolicySubscriptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RetentionPolicy", request.getContext().getTraceId(), "ListRetentionPolicySubscriptions");

        var outbound0 = ListRetentionPolicyConflictsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListRetentionPolicyConflictsRequest.Filter.newBuilder()
                        .setQuery(request.getFilter().getQuery())
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build();
        var reply0 = auditRetentionPolicySyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listRetentionPolicyConflicts(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListRetentionPolicySubscriptions", request.getPage().getPageToken());
        return ListRetentionPolicySubscriptionsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Sends a retention policy.
     */
    public SendRetentionPolicyTestNoticeResponse sendRetentionPolicyTestNotice(SendRetentionPolicyTestNoticeRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SendRetentionPolicyTestNotice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RetentionPolicy", request.getRetentionPolicyId(), "SendRetentionPolicyTestNotice");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getPhone()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SendRetentionPolicyTestNotice", request.getDetails().getRecipient().getPhone());
        return SendRetentionPolicyTestNoticeResponse.newBuilder()
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .build())
                        .setRetentionPolicyId(request.getRetentionPolicyId())
                        .build())
                .build();
    }

    /**
     * Returns a retention policy.
     */
    public GetRetentionPolicyPreferencesResponse getRetentionPolicyPreferences(GetRetentionPolicyPreferencesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetRetentionPolicyPreferences")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RetentionPolicy", request.getRetentionPolicyId(), "GetRetentionPolicyPreferences");

        var outbound0 = mapper.getRetentionPolicyPreferencesStage0(request);
        var reply0 = auditAuditTrailSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveAuditTrailConflict(outbound0);
        return GetRetentionPolicyPreferencesResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setRetentionPolicyId(request.getRetentionPolicyId())
                        .build())
                .build();
    }

    /**
     * Streams the changes of retention policies as they happen.
     */
    public StreamRetentionPolicyNoticesResponse streamRetentionPolicyNotices(StreamRetentionPolicyNoticesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamRetentionPolicyNotices")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RetentionPolicy", request.getContext().getTraceId(), "StreamRetentionPolicyNotices");
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamRetentionPolicyNotices", request.getFilter().getQuery());
        return StreamRetentionPolicyNoticesResponse.newBuilder()
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .build())
                        .build())
                .build();
    }
}
