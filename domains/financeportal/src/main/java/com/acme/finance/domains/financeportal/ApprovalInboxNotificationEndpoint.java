package com.acme.finance.domains.financeportal;

import com.acme.finance.platform.RpcSupport;
import com.acme.financeportal.v1.ApprovalInboxNotificationServiceGrpc;
import com.acme.financeportal.v1.GetApprovalInboxPreferencesRequest;
import com.acme.financeportal.v1.GetApprovalInboxPreferencesResponse;
import com.acme.financeportal.v1.ListApprovalInboxSubscriptionsRequest;
import com.acme.financeportal.v1.ListApprovalInboxSubscriptionsResponse;
import com.acme.financeportal.v1.NotifyApprovalInboxWatchersRequest;
import com.acme.financeportal.v1.NotifyApprovalInboxWatchersResponse;
import com.acme.financeportal.v1.SendApprovalInboxTestNoticeRequest;
import com.acme.financeportal.v1.SendApprovalInboxTestNoticeResponse;
import com.acme.financeportal.v1.StreamApprovalInboxNoticesRequest;
import com.acme.financeportal.v1.StreamApprovalInboxNoticesResponse;
import com.acme.financeportal.v1.SubscribeApprovalInboxUpdatesRequest;
import com.acme.financeportal.v1.SubscribeApprovalInboxUpdatesResponse;
import com.acme.financeportal.v1.UnsubscribeApprovalInboxUpdatesRequest;
import com.acme.financeportal.v1.UnsubscribeApprovalInboxUpdatesResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Tells the people who follow an approval inbox what changed.
 */
@GrpcService
public class ApprovalInboxNotificationEndpoint extends ApprovalInboxNotificationServiceGrpc.ApprovalInboxNotificationServiceImplBase {
    protected final ApprovalInboxNotificationApplication application;

    public ApprovalInboxNotificationEndpoint(ApprovalInboxNotificationApplication application) {
        this.application = application;
    }

    /**
     * Subscribes an approval inbox.
     */
    @Override
    public void subscribeApprovalInboxUpdates(SubscribeApprovalInboxUpdatesRequest request, StreamObserver<SubscribeApprovalInboxUpdatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.subscribeApprovalInboxUpdates(request));
    }

    /**
     * Unsubscribes an approval inbox.
     */
    @Override
    public void unsubscribeApprovalInboxUpdates(UnsubscribeApprovalInboxUpdatesRequest request, StreamObserver<UnsubscribeApprovalInboxUpdatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.unsubscribeApprovalInboxUpdates(request));
    }

    /**
     * Notifys an approval inbox.
     */
    @Override
    public void notifyApprovalInboxWatchers(NotifyApprovalInboxWatchersRequest request, StreamObserver<NotifyApprovalInboxWatchersResponse> observer) {
        RpcSupport.unary(observer, () -> application.notifyApprovalInboxWatchers(request));
    }

    /**
     * Lists approval inboxes a page at a time.
     */
    @Override
    public void listApprovalInboxSubscriptions(ListApprovalInboxSubscriptionsRequest request, StreamObserver<ListApprovalInboxSubscriptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listApprovalInboxSubscriptions(request));
    }

    /**
     * Sends an approval inbox.
     */
    @Override
    public void sendApprovalInboxTestNotice(SendApprovalInboxTestNoticeRequest request, StreamObserver<SendApprovalInboxTestNoticeResponse> observer) {
        RpcSupport.unary(observer, () -> application.sendApprovalInboxTestNotice(request));
    }

    /**
     * Returns an approval inbox.
     */
    @Override
    public void getApprovalInboxPreferences(GetApprovalInboxPreferencesRequest request, StreamObserver<GetApprovalInboxPreferencesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getApprovalInboxPreferences(request));
    }

    /**
     * Streams the changes of approval inboxes as they happen.
     */
    @Override
    public void streamApprovalInboxNotices(StreamApprovalInboxNoticesRequest request, StreamObserver<StreamApprovalInboxNoticesResponse> observer) {
        try {
            var snapshot = application.streamApprovalInboxNotices(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }
}
