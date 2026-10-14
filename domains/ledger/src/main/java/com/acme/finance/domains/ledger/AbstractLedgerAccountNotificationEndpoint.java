package com.acme.finance.domains.ledger;

import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.GetLedgerAccountPreferencesRequest;
import com.acme.ledger.v1.GetLedgerAccountPreferencesResponse;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.ListLedgerAccountSubscriptionsRequest;
import com.acme.ledger.v1.ListLedgerAccountSubscriptionsResponse;
import com.acme.ledger.v1.NotifyLedgerAccountWatchersRequest;
import com.acme.ledger.v1.NotifyLedgerAccountWatchersResponse;
import com.acme.ledger.v1.SendLedgerAccountTestNoticeRequest;
import com.acme.ledger.v1.SendLedgerAccountTestNoticeResponse;
import com.acme.ledger.v1.StreamLedgerAccountNoticesRequest;
import com.acme.ledger.v1.StreamLedgerAccountNoticesResponse;
import com.acme.ledger.v1.SubscribeLedgerAccountUpdatesRequest;
import com.acme.ledger.v1.SubscribeLedgerAccountUpdatesResponse;
import com.acme.ledger.v1.UnsubscribeLedgerAccountUpdatesRequest;
import com.acme.ledger.v1.UnsubscribeLedgerAccountUpdatesResponse;
import io.grpc.stub.StreamObserver;

/**
 * Tells the people who follow a ledger account what changed.
 */
public abstract class AbstractLedgerAccountNotificationEndpoint extends LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceImplBase {
    protected final LedgerAccountNotificationApplication application;

    protected AbstractLedgerAccountNotificationEndpoint(LedgerAccountNotificationApplication application) {
        this.application = application;
    }

    /**
     * Subscribes a ledger account.
     */
    @Override
    public void subscribeLedgerAccountUpdates(SubscribeLedgerAccountUpdatesRequest request, StreamObserver<SubscribeLedgerAccountUpdatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.subscribeLedgerAccountUpdates(request));
    }

    /**
     * Unsubscribes a ledger account.
     */
    @Override
    public void unsubscribeLedgerAccountUpdates(UnsubscribeLedgerAccountUpdatesRequest request, StreamObserver<UnsubscribeLedgerAccountUpdatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.unsubscribeLedgerAccountUpdates(request));
    }

    /**
     * Notifys a ledger account.
     */
    @Override
    public void notifyLedgerAccountWatchers(NotifyLedgerAccountWatchersRequest request, StreamObserver<NotifyLedgerAccountWatchersResponse> observer) {
        RpcSupport.unary(observer, () -> application.notifyLedgerAccountWatchers(request));
    }

    /**
     * Lists ledger accounts a page at a time.
     */
    @Override
    public void listLedgerAccountSubscriptions(ListLedgerAccountSubscriptionsRequest request, StreamObserver<ListLedgerAccountSubscriptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listLedgerAccountSubscriptions(request));
    }

    /**
     * Sends a ledger account.
     */
    @Override
    public void sendLedgerAccountTestNotice(SendLedgerAccountTestNoticeRequest request, StreamObserver<SendLedgerAccountTestNoticeResponse> observer) {
        RpcSupport.unary(observer, () -> application.sendLedgerAccountTestNotice(request));
    }

    /**
     * Returns a ledger account.
     */
    @Override
    public void getLedgerAccountPreferences(GetLedgerAccountPreferencesRequest request, StreamObserver<GetLedgerAccountPreferencesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getLedgerAccountPreferences(request));
    }

    /**
     * Streams the changes of ledger accounts as they happen.
     */
    @Override
    public void streamLedgerAccountNotices(StreamLedgerAccountNoticesRequest request, StreamObserver<StreamLedgerAccountNoticesResponse> observer) {
        try {
            var snapshot = application.streamLedgerAccountNotices(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }
}
