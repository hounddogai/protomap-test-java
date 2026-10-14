package com.acme.finance.domains.ledger;

import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.GetFiscalPeriodPreferencesRequest;
import com.acme.ledger.v1.GetFiscalPeriodPreferencesResponse;
import com.acme.ledger.v1.ListFiscalPeriodSubscriptionsRequest;
import com.acme.ledger.v1.ListFiscalPeriodSubscriptionsResponse;
import com.acme.ledger.v1.NotifyFiscalPeriodWatchersRequest;
import com.acme.ledger.v1.NotifyFiscalPeriodWatchersResponse;
import com.acme.ledger.v1.SendFiscalPeriodTestNoticeRequest;
import com.acme.ledger.v1.SendFiscalPeriodTestNoticeResponse;
import com.acme.ledger.v1.StreamFiscalPeriodNoticesRequest;
import com.acme.ledger.v1.StreamFiscalPeriodNoticesResponse;
import com.acme.ledger.v1.SubscribeFiscalPeriodUpdatesRequest;
import com.acme.ledger.v1.SubscribeFiscalPeriodUpdatesResponse;
import com.acme.ledger.v1.UnsubscribeFiscalPeriodUpdatesRequest;
import com.acme.ledger.v1.UnsubscribeFiscalPeriodUpdatesResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Tells the people who follow a fiscal period what changed.
 */
@GrpcService
public class FiscalPeriodNotificationEndpoint extends FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceImplBase {
    protected final FiscalPeriodNotificationApplication application;

    public FiscalPeriodNotificationEndpoint(FiscalPeriodNotificationApplication application) {
        this.application = application;
    }

    /**
     * Subscribes a fiscal period.
     */
    @Override
    public void subscribeFiscalPeriodUpdates(SubscribeFiscalPeriodUpdatesRequest request, StreamObserver<SubscribeFiscalPeriodUpdatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.subscribeFiscalPeriodUpdates(request));
    }

    /**
     * Unsubscribes a fiscal period.
     */
    @Override
    public void unsubscribeFiscalPeriodUpdates(UnsubscribeFiscalPeriodUpdatesRequest request, StreamObserver<UnsubscribeFiscalPeriodUpdatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.unsubscribeFiscalPeriodUpdates(request));
    }

    /**
     * Notifys a fiscal period.
     */
    @Override
    public void notifyFiscalPeriodWatchers(NotifyFiscalPeriodWatchersRequest request, StreamObserver<NotifyFiscalPeriodWatchersResponse> observer) {
        RpcSupport.unary(observer, () -> application.notifyFiscalPeriodWatchers(request));
    }

    /**
     * Lists fiscal periods a page at a time.
     */
    @Override
    public void listFiscalPeriodSubscriptions(ListFiscalPeriodSubscriptionsRequest request, StreamObserver<ListFiscalPeriodSubscriptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listFiscalPeriodSubscriptions(request));
    }

    /**
     * Sends a fiscal period.
     */
    @Override
    public void sendFiscalPeriodTestNotice(SendFiscalPeriodTestNoticeRequest request, StreamObserver<SendFiscalPeriodTestNoticeResponse> observer) {
        RpcSupport.unary(observer, () -> application.sendFiscalPeriodTestNotice(request));
    }

    /**
     * Returns a fiscal period.
     */
    @Override
    public void getFiscalPeriodPreferences(GetFiscalPeriodPreferencesRequest request, StreamObserver<GetFiscalPeriodPreferencesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getFiscalPeriodPreferences(request));
    }

    /**
     * Streams the changes of fiscal periods as they happen.
     */
    @Override
    public void streamFiscalPeriodNotices(StreamFiscalPeriodNoticesRequest request, StreamObserver<StreamFiscalPeriodNoticesResponse> observer) {
        try {
            var snapshot = application.streamFiscalPeriodNotices(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }
}
