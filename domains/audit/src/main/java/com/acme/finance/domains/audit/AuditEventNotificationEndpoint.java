package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.GetAuditEventPreferencesRequest;
import com.acme.audit.v1.GetAuditEventPreferencesResponse;
import com.acme.audit.v1.ListAuditEventSubscriptionsRequest;
import com.acme.audit.v1.ListAuditEventSubscriptionsResponse;
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
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Tells the people who follow an audit event what changed.
 */
@GrpcService
public class AuditEventNotificationEndpoint extends AuditEventNotificationServiceGrpc.AuditEventNotificationServiceImplBase {
    protected final AuditEventNotificationApplication application;

    public AuditEventNotificationEndpoint(AuditEventNotificationApplication application) {
        this.application = application;
    }

    /**
     * Subscribes an audit event.
     */
    @Override
    public void subscribeAuditEventUpdates(SubscribeAuditEventUpdatesRequest request, StreamObserver<SubscribeAuditEventUpdatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.subscribeAuditEventUpdates(request));
    }

    /**
     * Unsubscribes an audit event.
     */
    @Override
    public void unsubscribeAuditEventUpdates(UnsubscribeAuditEventUpdatesRequest request, StreamObserver<UnsubscribeAuditEventUpdatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.unsubscribeAuditEventUpdates(request));
    }

    /**
     * Notifys an audit event.
     */
    @Override
    public void notifyAuditEventWatchers(NotifyAuditEventWatchersRequest request, StreamObserver<NotifyAuditEventWatchersResponse> observer) {
        RpcSupport.unary(observer, () -> application.notifyAuditEventWatchers(request));
    }

    /**
     * Lists audit events a page at a time.
     */
    @Override
    public void listAuditEventSubscriptions(ListAuditEventSubscriptionsRequest request, StreamObserver<ListAuditEventSubscriptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listAuditEventSubscriptions(request));
    }

    /**
     * Sends an audit event.
     */
    @Override
    public void sendAuditEventTestNotice(SendAuditEventTestNoticeRequest request, StreamObserver<SendAuditEventTestNoticeResponse> observer) {
        RpcSupport.unary(observer, () -> application.sendAuditEventTestNotice(request));
    }

    /**
     * Returns an audit event.
     */
    @Override
    public void getAuditEventPreferences(GetAuditEventPreferencesRequest request, StreamObserver<GetAuditEventPreferencesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getAuditEventPreferences(request));
    }

    /**
     * Streams the changes of audit events as they happen.
     */
    @Override
    public void streamAuditEventNotices(StreamAuditEventNoticesRequest request, StreamObserver<StreamAuditEventNoticesResponse> observer) {
        try {
            var snapshot = application.streamAuditEventNotices(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }
}
