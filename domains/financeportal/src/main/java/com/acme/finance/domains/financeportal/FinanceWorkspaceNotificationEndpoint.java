package com.acme.finance.domains.financeportal;

import com.acme.finance.platform.RpcSupport;
import com.acme.financeportal.v1.FinanceWorkspaceNotificationServiceGrpc;
import com.acme.financeportal.v1.GetFinanceWorkspacePreferencesRequest;
import com.acme.financeportal.v1.GetFinanceWorkspacePreferencesResponse;
import com.acme.financeportal.v1.ListFinanceWorkspaceSubscriptionsRequest;
import com.acme.financeportal.v1.ListFinanceWorkspaceSubscriptionsResponse;
import com.acme.financeportal.v1.NotifyFinanceWorkspaceWatchersRequest;
import com.acme.financeportal.v1.NotifyFinanceWorkspaceWatchersResponse;
import com.acme.financeportal.v1.SendFinanceWorkspaceTestNoticeRequest;
import com.acme.financeportal.v1.SendFinanceWorkspaceTestNoticeResponse;
import com.acme.financeportal.v1.StreamFinanceWorkspaceNoticesRequest;
import com.acme.financeportal.v1.StreamFinanceWorkspaceNoticesResponse;
import com.acme.financeportal.v1.SubscribeFinanceWorkspaceUpdatesRequest;
import com.acme.financeportal.v1.SubscribeFinanceWorkspaceUpdatesResponse;
import com.acme.financeportal.v1.UnsubscribeFinanceWorkspaceUpdatesRequest;
import com.acme.financeportal.v1.UnsubscribeFinanceWorkspaceUpdatesResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Tells the people who follow a finance workspace what changed.
 */
@GrpcService
public class FinanceWorkspaceNotificationEndpoint extends FinanceWorkspaceNotificationServiceGrpc.FinanceWorkspaceNotificationServiceImplBase {
    protected final FinanceWorkspaceNotificationApplication application;

    public FinanceWorkspaceNotificationEndpoint(FinanceWorkspaceNotificationApplication application) {
        this.application = application;
    }

    /**
     * Subscribes a finance workspace.
     */
    @Override
    public void subscribeFinanceWorkspaceUpdates(SubscribeFinanceWorkspaceUpdatesRequest request, StreamObserver<SubscribeFinanceWorkspaceUpdatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.subscribeFinanceWorkspaceUpdates(request));
    }

    /**
     * Unsubscribes a finance workspace.
     */
    @Override
    public void unsubscribeFinanceWorkspaceUpdates(UnsubscribeFinanceWorkspaceUpdatesRequest request, StreamObserver<UnsubscribeFinanceWorkspaceUpdatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.unsubscribeFinanceWorkspaceUpdates(request));
    }

    /**
     * Notifys a finance workspace.
     */
    @Override
    public void notifyFinanceWorkspaceWatchers(NotifyFinanceWorkspaceWatchersRequest request, StreamObserver<NotifyFinanceWorkspaceWatchersResponse> observer) {
        RpcSupport.unary(observer, () -> application.notifyFinanceWorkspaceWatchers(request));
    }

    /**
     * Lists finance workspaces a page at a time.
     */
    @Override
    public void listFinanceWorkspaceSubscriptions(ListFinanceWorkspaceSubscriptionsRequest request, StreamObserver<ListFinanceWorkspaceSubscriptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listFinanceWorkspaceSubscriptions(request));
    }

    /**
     * Sends a finance workspace.
     */
    @Override
    public void sendFinanceWorkspaceTestNotice(SendFinanceWorkspaceTestNoticeRequest request, StreamObserver<SendFinanceWorkspaceTestNoticeResponse> observer) {
        RpcSupport.unary(observer, () -> application.sendFinanceWorkspaceTestNotice(request));
    }

    /**
     * Returns a finance workspace.
     */
    @Override
    public void getFinanceWorkspacePreferences(GetFinanceWorkspacePreferencesRequest request, StreamObserver<GetFinanceWorkspacePreferencesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getFinanceWorkspacePreferences(request));
    }

    /**
     * Streams the changes of finance workspaces as they happen.
     */
    @Override
    public void streamFinanceWorkspaceNotices(StreamFinanceWorkspaceNoticesRequest request, StreamObserver<StreamFinanceWorkspaceNoticesResponse> observer) {
        try {
            var snapshot = application.streamFinanceWorkspaceNotices(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }
}
