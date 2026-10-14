package com.acme.finance.domains.financeportal;

import com.acme.finance.platform.RpcSupport;
import com.acme.financeportal.v1.CloseChecklistNotificationServiceGrpc;
import com.acme.financeportal.v1.GetCloseChecklistPreferencesRequest;
import com.acme.financeportal.v1.GetCloseChecklistPreferencesResponse;
import com.acme.financeportal.v1.ListCloseChecklistSubscriptionsRequest;
import com.acme.financeportal.v1.ListCloseChecklistSubscriptionsResponse;
import com.acme.financeportal.v1.NotifyCloseChecklistWatchersRequest;
import com.acme.financeportal.v1.NotifyCloseChecklistWatchersResponse;
import com.acme.financeportal.v1.SendCloseChecklistTestNoticeRequest;
import com.acme.financeportal.v1.SendCloseChecklistTestNoticeResponse;
import com.acme.financeportal.v1.StreamCloseChecklistNoticesRequest;
import com.acme.financeportal.v1.StreamCloseChecklistNoticesResponse;
import com.acme.financeportal.v1.SubscribeCloseChecklistUpdatesRequest;
import com.acme.financeportal.v1.SubscribeCloseChecklistUpdatesResponse;
import com.acme.financeportal.v1.UnsubscribeCloseChecklistUpdatesRequest;
import com.acme.financeportal.v1.UnsubscribeCloseChecklistUpdatesResponse;
import io.grpc.stub.StreamObserver;

/**
 * Tells the people who follow a close checklist what changed.
 */
public abstract class AbstractCloseChecklistNotificationEndpoint extends CloseChecklistNotificationServiceGrpc.CloseChecklistNotificationServiceImplBase {
    protected final CloseChecklistNotificationApplication application;

    protected AbstractCloseChecklistNotificationEndpoint(CloseChecklistNotificationApplication application) {
        this.application = application;
    }

    /**
     * Subscribes a close checklist.
     */
    @Override
    public void subscribeCloseChecklistUpdates(SubscribeCloseChecklistUpdatesRequest request, StreamObserver<SubscribeCloseChecklistUpdatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.subscribeCloseChecklistUpdates(request));
    }

    /**
     * Unsubscribes a close checklist.
     */
    @Override
    public void unsubscribeCloseChecklistUpdates(UnsubscribeCloseChecklistUpdatesRequest request, StreamObserver<UnsubscribeCloseChecklistUpdatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.unsubscribeCloseChecklistUpdates(request));
    }

    /**
     * Notifys a close checklist.
     */
    @Override
    public void notifyCloseChecklistWatchers(NotifyCloseChecklistWatchersRequest request, StreamObserver<NotifyCloseChecklistWatchersResponse> observer) {
        RpcSupport.unary(observer, () -> application.notifyCloseChecklistWatchers(request));
    }

    /**
     * Lists close checklists a page at a time.
     */
    @Override
    public void listCloseChecklistSubscriptions(ListCloseChecklistSubscriptionsRequest request, StreamObserver<ListCloseChecklistSubscriptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listCloseChecklistSubscriptions(request));
    }

    /**
     * Sends a close checklist.
     */
    @Override
    public void sendCloseChecklistTestNotice(SendCloseChecklistTestNoticeRequest request, StreamObserver<SendCloseChecklistTestNoticeResponse> observer) {
        RpcSupport.unary(observer, () -> application.sendCloseChecklistTestNotice(request));
    }

    /**
     * Returns a close checklist.
     */
    @Override
    public void getCloseChecklistPreferences(GetCloseChecklistPreferencesRequest request, StreamObserver<GetCloseChecklistPreferencesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getCloseChecklistPreferences(request));
    }

    /**
     * Streams the changes of close checklists as they happen.
     */
    @Override
    public void streamCloseChecklistNotices(StreamCloseChecklistNoticesRequest request, StreamObserver<StreamCloseChecklistNoticesResponse> observer) {
        try {
            var snapshot = application.streamCloseChecklistNotices(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }
}
