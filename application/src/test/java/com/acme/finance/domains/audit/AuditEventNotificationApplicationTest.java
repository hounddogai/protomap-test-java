package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailSyncServiceGrpc;
import com.acme.audit.v1.GetAuditEventPreferencesRequest;
import com.acme.audit.v1.GetAuditEventPreferencesResponse;
import com.acme.audit.v1.ListAuditEventSubscriptionsRequest;
import com.acme.audit.v1.ListAuditEventSubscriptionsResponse;
import com.acme.audit.v1.ListAuditTrailConflictsRequest;
import com.acme.audit.v1.ListAuditTrailConflictsResponse;
import com.acme.audit.v1.NotifyAuditEventWatchersRequest;
import com.acme.audit.v1.NotifyAuditEventWatchersResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.SendAuditEventTestNoticeRequest;
import com.acme.audit.v1.SendAuditEventTestNoticeResponse;
import com.acme.audit.v1.StreamAuditEventNoticesRequest;
import com.acme.audit.v1.StreamAuditEventNoticesResponse;
import com.acme.audit.v1.SubscribeAuditEventUpdatesRequest;
import com.acme.audit.v1.SubscribeAuditEventUpdatesResponse;
import com.acme.audit.v1.UnsubscribeAuditEventUpdatesRequest;
import com.acme.audit.v1.UnsubscribeAuditEventUpdatesResponse;
import com.acme.common.v1.OperationResult;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.notifications.v1.SearchNotificationsRequest;
import com.acme.notifications.v1.SearchNotificationsResponse;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class AuditEventNotificationApplicationTest {
    @Test
    void subscribeAuditEventUpdatesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AuditRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAuditEvent(String tenant, AuditEvent value) {}
            public void saveAuditTrail(String tenant, AuditTrail value) {}
            public void saveRetentionPolicy(String tenant, RetentionPolicy value) {}
        };
        var application = new AuditEventNotificationApplication(
                AuditTrailSyncServiceGrpc.newBlockingStub(channel),
                new AuditEventNotificationRequestMapperImpl(),
                NotificationServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(SubscribeAuditEventUpdatesRequest.getDefaultInstance());
        var response = application.subscribeAuditEventUpdates(request);
        var sent0 = channel.request("acme.notifications.v1.NotificationService/SearchNotifications");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "template_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "deadline"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationService/SearchNotifications"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "audit_event_id"),
                ProbeSupport.value(response, "audit_event", "audit_event_id"));
    }

    @Test
    void streamAuditEventNoticesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AuditRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAuditEvent(String tenant, AuditEvent value) {}
            public void saveAuditTrail(String tenant, AuditTrail value) {}
            public void saveRetentionPolicy(String tenant, RetentionPolicy value) {}
        };
        var application = new AuditEventNotificationApplication(
                AuditTrailSyncServiceGrpc.newBlockingStub(channel),
                new AuditEventNotificationRequestMapperImpl(),
                NotificationServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(StreamAuditEventNoticesRequest.getDefaultInstance());
        var response = application.streamAuditEventNotices(request);
        var sent0 = channel.request("acme.audit.v1.AuditTrailSyncService/ListAuditTrailConflicts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "query"),
                ProbeSupport.value(sent0, "filter", "query"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "owner_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "created_after"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailSyncService/ListAuditTrailConflicts"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailSyncService/ListAuditTrailConflicts"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        var endpoint = new AuditEventNotificationEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<StreamAuditEventNoticesResponse> observer = new StreamObserver<>() {
            public void onNext(StreamAuditEventNoticesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.streamAuditEventNotices(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }
}
