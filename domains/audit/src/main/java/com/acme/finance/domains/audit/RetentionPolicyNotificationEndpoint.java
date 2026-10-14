package com.acme.finance.domains.audit;

import com.acme.audit.v1.GetRetentionPolicyPreferencesRequest;
import com.acme.audit.v1.GetRetentionPolicyPreferencesResponse;
import com.acme.audit.v1.ListRetentionPolicySubscriptionsRequest;
import com.acme.audit.v1.ListRetentionPolicySubscriptionsResponse;
import com.acme.audit.v1.NotifyRetentionPolicyWatchersRequest;
import com.acme.audit.v1.NotifyRetentionPolicyWatchersResponse;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.SendRetentionPolicyTestNoticeRequest;
import com.acme.audit.v1.SendRetentionPolicyTestNoticeResponse;
import com.acme.audit.v1.StreamRetentionPolicyNoticesRequest;
import com.acme.audit.v1.StreamRetentionPolicyNoticesResponse;
import com.acme.audit.v1.SubscribeRetentionPolicyUpdatesRequest;
import com.acme.audit.v1.SubscribeRetentionPolicyUpdatesResponse;
import com.acme.audit.v1.UnsubscribeRetentionPolicyUpdatesRequest;
import com.acme.audit.v1.UnsubscribeRetentionPolicyUpdatesResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Tells the people who follow a retention policy what changed.
 */
@GrpcService
public class RetentionPolicyNotificationEndpoint extends RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceImplBase {
    protected final RetentionPolicyNotificationApplication application;

    public RetentionPolicyNotificationEndpoint(RetentionPolicyNotificationApplication application) {
        this.application = application;
    }

    /**
     * Subscribes a retention policy.
     */
    @Override
    public void subscribeRetentionPolicyUpdates(SubscribeRetentionPolicyUpdatesRequest request, StreamObserver<SubscribeRetentionPolicyUpdatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.subscribeRetentionPolicyUpdates(request));
    }

    /**
     * Unsubscribes a retention policy.
     */
    @Override
    public void unsubscribeRetentionPolicyUpdates(UnsubscribeRetentionPolicyUpdatesRequest request, StreamObserver<UnsubscribeRetentionPolicyUpdatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.unsubscribeRetentionPolicyUpdates(request));
    }

    /**
     * Notifys a retention policy.
     */
    @Override
    public void notifyRetentionPolicyWatchers(NotifyRetentionPolicyWatchersRequest request, StreamObserver<NotifyRetentionPolicyWatchersResponse> observer) {
        RpcSupport.unary(observer, () -> application.notifyRetentionPolicyWatchers(request));
    }

    /**
     * Lists retention policies a page at a time.
     */
    @Override
    public void listRetentionPolicySubscriptions(ListRetentionPolicySubscriptionsRequest request, StreamObserver<ListRetentionPolicySubscriptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listRetentionPolicySubscriptions(request));
    }

    /**
     * Sends a retention policy.
     */
    @Override
    public void sendRetentionPolicyTestNotice(SendRetentionPolicyTestNoticeRequest request, StreamObserver<SendRetentionPolicyTestNoticeResponse> observer) {
        RpcSupport.unary(observer, () -> application.sendRetentionPolicyTestNotice(request));
    }

    /**
     * Returns a retention policy.
     */
    @Override
    public void getRetentionPolicyPreferences(GetRetentionPolicyPreferencesRequest request, StreamObserver<GetRetentionPolicyPreferencesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getRetentionPolicyPreferences(request));
    }

    /**
     * Streams the changes of retention policies as they happen.
     */
    @Override
    public void streamRetentionPolicyNotices(StreamRetentionPolicyNoticesRequest request, StreamObserver<StreamRetentionPolicyNoticesResponse> observer) {
        try {
            var snapshot = application.streamRetentionPolicyNotices(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }
}
