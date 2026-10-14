package com.acme.finance.domains.ledger;

import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.GetPostingBatchPreferencesRequest;
import com.acme.ledger.v1.GetPostingBatchPreferencesResponse;
import com.acme.ledger.v1.ListPostingBatchSubscriptionsRequest;
import com.acme.ledger.v1.ListPostingBatchSubscriptionsResponse;
import com.acme.ledger.v1.NotifyPostingBatchWatchersRequest;
import com.acme.ledger.v1.NotifyPostingBatchWatchersResponse;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.SendPostingBatchTestNoticeRequest;
import com.acme.ledger.v1.SendPostingBatchTestNoticeResponse;
import com.acme.ledger.v1.StreamPostingBatchNoticesRequest;
import com.acme.ledger.v1.StreamPostingBatchNoticesResponse;
import com.acme.ledger.v1.SubscribePostingBatchUpdatesRequest;
import com.acme.ledger.v1.SubscribePostingBatchUpdatesResponse;
import com.acme.ledger.v1.UnsubscribePostingBatchUpdatesRequest;
import com.acme.ledger.v1.UnsubscribePostingBatchUpdatesResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Tells the people who follow a posting batch what changed.
 */
@GrpcService
public class PostingBatchNotificationEndpoint extends PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceImplBase {
    protected final PostingBatchNotificationApplication application;

    public PostingBatchNotificationEndpoint(PostingBatchNotificationApplication application) {
        this.application = application;
    }

    /**
     * Subscribes a posting batch.
     */
    @Override
    public void subscribePostingBatchUpdates(SubscribePostingBatchUpdatesRequest request, StreamObserver<SubscribePostingBatchUpdatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.subscribePostingBatchUpdates(request));
    }

    /**
     * Unsubscribes a posting batch.
     */
    @Override
    public void unsubscribePostingBatchUpdates(UnsubscribePostingBatchUpdatesRequest request, StreamObserver<UnsubscribePostingBatchUpdatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.unsubscribePostingBatchUpdates(request));
    }

    /**
     * Notifys a posting batch.
     */
    @Override
    public void notifyPostingBatchWatchers(NotifyPostingBatchWatchersRequest request, StreamObserver<NotifyPostingBatchWatchersResponse> observer) {
        RpcSupport.unary(observer, () -> application.notifyPostingBatchWatchers(request));
    }

    /**
     * Lists posting batches a page at a time.
     */
    @Override
    public void listPostingBatchSubscriptions(ListPostingBatchSubscriptionsRequest request, StreamObserver<ListPostingBatchSubscriptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPostingBatchSubscriptions(request));
    }

    /**
     * Sends a posting batch.
     */
    @Override
    public void sendPostingBatchTestNotice(SendPostingBatchTestNoticeRequest request, StreamObserver<SendPostingBatchTestNoticeResponse> observer) {
        RpcSupport.unary(observer, () -> application.sendPostingBatchTestNotice(request));
    }

    /**
     * Returns a posting batch.
     */
    @Override
    public void getPostingBatchPreferences(GetPostingBatchPreferencesRequest request, StreamObserver<GetPostingBatchPreferencesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getPostingBatchPreferences(request));
    }

    /**
     * Streams the changes of posting batches as they happen.
     */
    @Override
    public void streamPostingBatchNotices(StreamPostingBatchNoticesRequest request, StreamObserver<StreamPostingBatchNoticesResponse> observer) {
        try {
            var snapshot = application.streamPostingBatchNotices(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }
}
