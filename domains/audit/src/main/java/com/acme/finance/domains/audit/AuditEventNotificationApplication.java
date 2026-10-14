package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditTrailSyncServiceGrpc;
import com.acme.audit.v1.GetAuditEventPreferencesRequest;
import com.acme.audit.v1.GetAuditEventPreferencesResponse;
import com.acme.audit.v1.ListAuditEventSubscriptionsRequest;
import com.acme.audit.v1.ListAuditEventSubscriptionsResponse;
import com.acme.audit.v1.ListAuditTrailConflictsRequest;
import com.acme.audit.v1.ListAuditTrailConflictsResponse;
import com.acme.audit.v1.NotifyAuditEventWatchersRequest;
import com.acme.audit.v1.NotifyAuditEventWatchersResponse;
import com.acme.audit.v1.SendAuditEventTestNoticeRequest;
import com.acme.audit.v1.SendAuditEventTestNoticeResponse;
import com.acme.audit.v1.StreamAuditEventNoticesRequest;
import com.acme.audit.v1.StreamAuditEventNoticesResponse;
import com.acme.audit.v1.SubscribeAuditEventUpdatesRequest;
import com.acme.audit.v1.SubscribeAuditEventUpdatesResponse;
import com.acme.audit.v1.UnsubscribeAuditEventUpdatesRequest;
import com.acme.audit.v1.UnsubscribeAuditEventUpdatesResponse;
import com.acme.common.v1.OperationResult;
import com.acme.finance.platform.RpcSupport;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.notifications.v1.SearchNotificationsRequest;
import com.acme.notifications.v1.SearchNotificationsResponse;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Tells the people who follow an audit event what changed. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class AuditEventNotificationApplication {
    private static final Logger log = LoggerFactory.getLogger(AuditEventNotificationApplication.class);

    private final AuditTrailSyncServiceGrpc.AuditTrailSyncServiceBlockingStub auditAuditTrailSyncServiceBlocking;

    private final AuditEventNotificationRequestMapper mapper;

    private final NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking;

    private final AuditRepository repository;

    public AuditEventNotificationApplication(
            AuditTrailSyncServiceGrpc.AuditTrailSyncServiceBlockingStub auditAuditTrailSyncServiceBlocking,
            AuditEventNotificationRequestMapper mapper,
            NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking,
            AuditRepository repository) {
        this.auditAuditTrailSyncServiceBlocking = auditAuditTrailSyncServiceBlocking;
        this.mapper = mapper;
        this.notificationsNotificationServiceBlocking = notificationsNotificationServiceBlocking;
        this.repository = repository;
    }

    /**
     * Subscribes an audit event.
     */
    public SubscribeAuditEventUpdatesResponse subscribeAuditEventUpdates(SubscribeAuditEventUpdatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubscribeAuditEventUpdates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditEvent", request.getAuditEventId(), "SubscribeAuditEventUpdates");

        var outbound0 = SearchNotificationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchNotificationsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getDetails().getTemplateId())
                        .build())
                .build();
        final class Stage0Adapter {
            SearchNotificationsResponse fetch(SearchNotificationsRequest input) {
                return notificationsNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchNotifications(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getEmail()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubscribeAuditEventUpdates", request.getDetails().getRecipient().getEmail());
        return SubscribeAuditEventUpdatesResponse.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setAuditEventId(request.getAuditEventId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Unsubscribes an audit event.
     */
    public UnsubscribeAuditEventUpdatesResponse unsubscribeAuditEventUpdates(UnsubscribeAuditEventUpdatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "UnsubscribeAuditEventUpdates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditEvent", request.getAuditEventId(), "UnsubscribeAuditEventUpdates");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getEmail()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "UnsubscribeAuditEventUpdates", request.getDetails().getRecipient().getEmail());
        return UnsubscribeAuditEventUpdatesResponse.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setAuditEventId(request.getAuditEventId())
                        .build())
                .build();
    }

    /**
     * Notifys an audit event.
     */
    public NotifyAuditEventWatchersResponse notifyAuditEventWatchers(NotifyAuditEventWatchersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NotifyAuditEventWatchers")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditEvent", request.getAuditEventId(), "NotifyAuditEventWatchers");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getEmail()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NotifyAuditEventWatchers", request.getDetails().getRecipient().getEmail());
        return NotifyAuditEventWatchersResponse.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setAuditEventId(request.getAuditEventId())
                        .build())
                .build();
    }

    /**
     * Lists audit events a page at a time.
     */
    public ListAuditEventSubscriptionsResponse listAuditEventSubscriptions(ListAuditEventSubscriptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListAuditEventSubscriptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditEvent", request.getContext().getTraceId(), "ListAuditEventSubscriptions");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListAuditEventSubscriptions", request.getPage().getPageToken());
        return ListAuditEventSubscriptionsResponse.getDefaultInstance();
    }

    /**
     * Sends an audit event.
     */
    public SendAuditEventTestNoticeResponse sendAuditEventTestNotice(SendAuditEventTestNoticeRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SendAuditEventTestNotice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditEvent", request.getAuditEventId(), "SendAuditEventTestNotice");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getEmail()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SendAuditEventTestNotice", request.getDetails().getRecipient().getEmail());
        return SendAuditEventTestNoticeResponse.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setAuditEventId(request.getAuditEventId())
                        .build())
                .build();
    }

    /**
     * Returns an audit event.
     */
    public GetAuditEventPreferencesResponse getAuditEventPreferences(GetAuditEventPreferencesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetAuditEventPreferences")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditEvent", request.getAuditEventId(), "GetAuditEventPreferences");
        return GetAuditEventPreferencesResponse.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setAuditEventId(request.getAuditEventId())
                        .build())
                .build();
    }

    /**
     * Streams the changes of audit events as they happen.
     */
    public StreamAuditEventNoticesResponse streamAuditEventNotices(StreamAuditEventNoticesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamAuditEventNotices")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditEvent", request.getContext().getTraceId(), "StreamAuditEventNotices");

        var outbound0 = mapper.streamAuditEventNoticesStage0(request);
        var reply0 = auditAuditTrailSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listAuditTrailConflicts(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getSubscriber().getEmail()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamAuditEventNotices", request.getSubscriber().getEmail());
        return StreamAuditEventNoticesResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }
}
