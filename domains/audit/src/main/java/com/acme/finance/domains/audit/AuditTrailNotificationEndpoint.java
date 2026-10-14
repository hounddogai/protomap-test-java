package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditTrailNotificationServiceGrpc;
import com.acme.audit.v1.GetAuditTrailPreferencesRequest;
import com.acme.audit.v1.GetAuditTrailPreferencesResponse;
import com.acme.audit.v1.ListAuditTrailSubscriptionsRequest;
import com.acme.audit.v1.ListAuditTrailSubscriptionsResponse;
import com.acme.audit.v1.NotifyAuditTrailWatchersRequest;
import com.acme.audit.v1.NotifyAuditTrailWatchersResponse;
import com.acme.audit.v1.SendAuditTrailTestNoticeRequest;
import com.acme.audit.v1.SendAuditTrailTestNoticeResponse;
import com.acme.audit.v1.StreamAuditTrailNoticesRequest;
import com.acme.audit.v1.StreamAuditTrailNoticesResponse;
import com.acme.audit.v1.SubscribeAuditTrailUpdatesRequest;
import com.acme.audit.v1.SubscribeAuditTrailUpdatesResponse;
import com.acme.audit.v1.UnsubscribeAuditTrailUpdatesRequest;
import com.acme.audit.v1.UnsubscribeAuditTrailUpdatesResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Tells the people who follow an audit trail what changed.
 */
@GrpcService
public class AuditTrailNotificationEndpoint extends AuditTrailNotificationServiceGrpc.AuditTrailNotificationServiceImplBase {
    protected final AuditTrailNotificationApplication application;

    public AuditTrailNotificationEndpoint(AuditTrailNotificationApplication application) {
        this.application = application;
    }

    /**
     * Subscribes an audit trail.
     */
    @Override
    public void subscribeAuditTrailUpdates(SubscribeAuditTrailUpdatesRequest request, StreamObserver<SubscribeAuditTrailUpdatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.subscribeAuditTrailUpdates(request));
    }

    /**
     * Unsubscribes an audit trail.
     */
    @Override
    public void unsubscribeAuditTrailUpdates(UnsubscribeAuditTrailUpdatesRequest request, StreamObserver<UnsubscribeAuditTrailUpdatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.unsubscribeAuditTrailUpdates(request));
    }

    /**
     * Notifys an audit trail.
     */
    @Override
    public void notifyAuditTrailWatchers(NotifyAuditTrailWatchersRequest request, StreamObserver<NotifyAuditTrailWatchersResponse> observer) {
        RpcSupport.unary(observer, () -> application.notifyAuditTrailWatchers(request));
    }

    /**
     * Lists audit trails a page at a time.
     */
    @Override
    public void listAuditTrailSubscriptions(ListAuditTrailSubscriptionsRequest request, StreamObserver<ListAuditTrailSubscriptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listAuditTrailSubscriptions(request));
    }

    /**
     * Sends an audit trail.
     */
    @Override
    public void sendAuditTrailTestNotice(SendAuditTrailTestNoticeRequest request, StreamObserver<SendAuditTrailTestNoticeResponse> observer) {
        RpcSupport.unary(observer, () -> application.sendAuditTrailTestNotice(request));
    }

    /**
     * Returns an audit trail.
     */
    @Override
    public void getAuditTrailPreferences(GetAuditTrailPreferencesRequest request, StreamObserver<GetAuditTrailPreferencesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getAuditTrailPreferences(request));
    }

    /**
     * Streams the changes of audit trails as they happen.
     */
    @Override
    public void streamAuditTrailNotices(StreamAuditTrailNoticesRequest request, StreamObserver<StreamAuditTrailNoticesResponse> observer) {
        try {
            var snapshot = application.streamAuditTrailNotices(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }
}
