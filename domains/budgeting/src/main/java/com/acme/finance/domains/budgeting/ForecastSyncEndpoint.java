package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.ForecastSyncServiceGrpc;
import com.acme.budgeting.v1.GetForecastSyncStateRequest;
import com.acme.budgeting.v1.GetForecastSyncStateResponse;
import com.acme.budgeting.v1.ListForecastConflictsRequest;
import com.acme.budgeting.v1.ListForecastConflictsResponse;
import com.acme.budgeting.v1.PullForecastsRequest;
import com.acme.budgeting.v1.PullForecastsResponse;
import com.acme.budgeting.v1.PushForecastRequest;
import com.acme.budgeting.v1.PushForecastResponse;
import com.acme.budgeting.v1.ReconcileForecastsRequest;
import com.acme.budgeting.v1.ReconcileForecastsResponse;
import com.acme.budgeting.v1.ResolveForecastConflictRequest;
import com.acme.budgeting.v1.ResolveForecastConflictResponse;
import com.acme.budgeting.v1.StreamForecastChangesRequest;
import com.acme.budgeting.v1.StreamForecastChangesResponse;
import com.acme.budgeting.v1.SyncForecastsRequest;
import com.acme.budgeting.v1.SyncForecastsResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Keeps forecasts in step with the systems of record in other regions.
 */
@GrpcService
public class ForecastSyncEndpoint extends ForecastSyncServiceGrpc.ForecastSyncServiceImplBase {
    protected final ForecastSyncApplication application;

    public ForecastSyncEndpoint(ForecastSyncApplication application) {
        this.application = application;
    }

    /**
     * Syncs a forecast.
     */
    @Override
    public void syncForecasts(SyncForecastsRequest request, StreamObserver<SyncForecastsResponse> observer) {
        RpcSupport.unary(observer, () -> application.syncForecasts(request));
    }

    /**
     * Pushs a forecast.
     */
    @Override
    public void pushForecast(PushForecastRequest request, StreamObserver<PushForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.pushForecast(request));
    }

    /**
     * Pulls a forecast.
     */
    @Override
    public void pullForecasts(PullForecastsRequest request, StreamObserver<PullForecastsResponse> observer) {
        RpcSupport.unary(observer, () -> application.pullForecasts(request));
    }

    /**
     * Reconciles a forecast.
     */
    @Override
    public void reconcileForecasts(ReconcileForecastsRequest request, StreamObserver<ReconcileForecastsResponse> observer) {
        RpcSupport.unary(observer, () -> application.reconcileForecasts(request));
    }

    /**
     * Exchanges changes of forecasts with a peer.
     */
    @Override
    public StreamObserver<StreamForecastChangesRequest> streamForecastChanges(StreamObserver<StreamForecastChangesResponse> observer) {
        return new StreamObserver<StreamForecastChangesRequest>() {
            private boolean finished;
            private final StreamForecastChangesResponse.Builder summary = StreamForecastChangesResponse.newBuilder();

            @Override
            public void onNext(StreamForecastChangesRequest request) {
                if (finished) { return; }
                try {
                    var response = application.streamForecastChanges(request);
                    observer.onNext(response);
                } catch (RuntimeException failure) {
                    finished = true;
                    observer.onError(RpcSupport.status(failure));
                }
            }

            @Override
            public void onError(Throwable failure) {
                if (!finished) {
                    finished = true;
                    observer.onError(RpcSupport.status(failure));
                }
            }

            @Override
            public void onCompleted() {
                if (finished) { return; }
                finished = true;
                observer.onCompleted();
            }
        };
    }

    /**
     * Resolves a forecast.
     */
    @Override
    public void resolveForecastConflict(ResolveForecastConflictRequest request, StreamObserver<ResolveForecastConflictResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveForecastConflict(request));
    }

    /**
     * Returns a forecast.
     */
    @Override
    public void getForecastSyncState(GetForecastSyncStateRequest request, StreamObserver<GetForecastSyncStateResponse> observer) {
        RpcSupport.unary(observer, () -> application.getForecastSyncState(request));
    }

    /**
     * Lists forecasts a page at a time.
     */
    @Override
    public void listForecastConflicts(ListForecastConflictsRequest request, StreamObserver<ListForecastConflictsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listForecastConflicts(request));
    }
}
