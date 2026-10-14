package com.acme.finance.domains.ledger;

import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.GetJournalEntryPreferencesRequest;
import com.acme.ledger.v1.GetJournalEntryPreferencesResponse;
import com.acme.ledger.v1.JournalEntryNotificationServiceGrpc;
import com.acme.ledger.v1.ListJournalEntrySubscriptionsRequest;
import com.acme.ledger.v1.ListJournalEntrySubscriptionsResponse;
import com.acme.ledger.v1.NotifyJournalEntryWatchersRequest;
import com.acme.ledger.v1.NotifyJournalEntryWatchersResponse;
import com.acme.ledger.v1.SendJournalEntryTestNoticeRequest;
import com.acme.ledger.v1.SendJournalEntryTestNoticeResponse;
import com.acme.ledger.v1.StreamJournalEntryNoticesRequest;
import com.acme.ledger.v1.StreamJournalEntryNoticesResponse;
import com.acme.ledger.v1.SubscribeJournalEntryUpdatesRequest;
import com.acme.ledger.v1.SubscribeJournalEntryUpdatesResponse;
import com.acme.ledger.v1.UnsubscribeJournalEntryUpdatesRequest;
import com.acme.ledger.v1.UnsubscribeJournalEntryUpdatesResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Tells the people who follow a journal entry what changed.
 */
@GrpcService
public class JournalEntryNotificationEndpoint extends JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceImplBase {
    protected final JournalEntryNotificationApplication application;

    public JournalEntryNotificationEndpoint(JournalEntryNotificationApplication application) {
        this.application = application;
    }

    /**
     * Subscribes a journal entry.
     */
    @Override
    public void subscribeJournalEntryUpdates(SubscribeJournalEntryUpdatesRequest request, StreamObserver<SubscribeJournalEntryUpdatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.subscribeJournalEntryUpdates(request));
    }

    /**
     * Unsubscribes a journal entry.
     */
    @Override
    public void unsubscribeJournalEntryUpdates(UnsubscribeJournalEntryUpdatesRequest request, StreamObserver<UnsubscribeJournalEntryUpdatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.unsubscribeJournalEntryUpdates(request));
    }

    /**
     * Notifys a journal entry.
     */
    @Override
    public void notifyJournalEntryWatchers(NotifyJournalEntryWatchersRequest request, StreamObserver<NotifyJournalEntryWatchersResponse> observer) {
        RpcSupport.unary(observer, () -> application.notifyJournalEntryWatchers(request));
    }

    /**
     * Lists journal entries a page at a time.
     */
    @Override
    public void listJournalEntrySubscriptions(ListJournalEntrySubscriptionsRequest request, StreamObserver<ListJournalEntrySubscriptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listJournalEntrySubscriptions(request));
    }

    /**
     * Sends a journal entry.
     */
    @Override
    public void sendJournalEntryTestNotice(SendJournalEntryTestNoticeRequest request, StreamObserver<SendJournalEntryTestNoticeResponse> observer) {
        RpcSupport.unary(observer, () -> application.sendJournalEntryTestNotice(request));
    }

    /**
     * Returns a journal entry.
     */
    @Override
    public void getJournalEntryPreferences(GetJournalEntryPreferencesRequest request, StreamObserver<GetJournalEntryPreferencesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getJournalEntryPreferences(request));
    }

    /**
     * Streams the changes of journal entries as they happen.
     */
    @Override
    public void streamJournalEntryNotices(StreamJournalEntryNoticesRequest request, StreamObserver<StreamJournalEntryNoticesResponse> observer) {
        try {
            var snapshot = application.streamJournalEntryNotices(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }
}
