package com.acme.finance.domains.treasury;

import com.acme.finance.platform.RpcSupport;
import com.acme.treasury.v1.GetLiquidityForecastSyncStateRequest;
import com.acme.treasury.v1.GetLiquidityForecastSyncStateResponse;
import com.acme.treasury.v1.LiquidityForecastSyncServiceGrpc;
import com.acme.treasury.v1.ListLiquidityForecastConflictsRequest;
import com.acme.treasury.v1.ListLiquidityForecastConflictsResponse;
import com.acme.treasury.v1.PullLiquidityForecastsRequest;
import com.acme.treasury.v1.PullLiquidityForecastsResponse;
import com.acme.treasury.v1.PushLiquidityForecastRequest;
import com.acme.treasury.v1.PushLiquidityForecastResponse;
import com.acme.treasury.v1.ResolveLiquidityForecastConflictRequest;
import com.acme.treasury.v1.ResolveLiquidityForecastConflictResponse;
import com.acme.treasury.v1.StreamLiquidityForecastChangesRequest;
import com.acme.treasury.v1.StreamLiquidityForecastChangesResponse;
import com.acme.treasury.v1.SyncLiquidityForecastsRequest;
import com.acme.treasury.v1.SyncLiquidityForecastsResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Keeps liquidity forecasts in step with the systems of record in other regions.
 */
@GrpcService
public class LiquidityForecastSyncEndpoint extends LiquidityForecastSyncServiceGrpc.LiquidityForecastSyncServiceImplBase {
    protected final LiquidityForecastSyncApplication application;

    public LiquidityForecastSyncEndpoint(LiquidityForecastSyncApplication application) {
        this.application = application;
    }

    /**
     * Syncs a liquidity forecast.
     */
    @Override
    public void syncLiquidityForecasts(SyncLiquidityForecastsRequest request, StreamObserver<SyncLiquidityForecastsResponse> observer) {
        RpcSupport.unary(observer, () -> application.syncLiquidityForecasts(request));
    }

    /**
     * Pushs a liquidity forecast.
     */
    @Override
    public void pushLiquidityForecast(PushLiquidityForecastRequest request, StreamObserver<PushLiquidityForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.pushLiquidityForecast(request));
    }

    /**
     * Pulls a liquidity forecast.
     */
    @Override
    public void pullLiquidityForecasts(PullLiquidityForecastsRequest request, StreamObserver<PullLiquidityForecastsResponse> observer) {
        RpcSupport.unary(observer, () -> application.pullLiquidityForecasts(request));
    }

    /**
     * Exchanges changes of liquidity forecasts with a peer.
     */
    @Override
    public StreamObserver<StreamLiquidityForecastChangesRequest> streamLiquidityForecastChanges(StreamObserver<StreamLiquidityForecastChangesResponse> observer) {
        return new StreamObserver<StreamLiquidityForecastChangesRequest>() {
            private boolean finished;
            private final StreamLiquidityForecastChangesResponse.Builder summary = StreamLiquidityForecastChangesResponse.newBuilder();

            @Override
            public void onNext(StreamLiquidityForecastChangesRequest request) {
                if (finished) { return; }
                try {
                    var response = application.streamLiquidityForecastChanges(request);
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
     * Resolves a liquidity forecast.
     */
    @Override
    public void resolveLiquidityForecastConflict(ResolveLiquidityForecastConflictRequest request, StreamObserver<ResolveLiquidityForecastConflictResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveLiquidityForecastConflict(request));
    }

    /**
     * Returns a liquidity forecast.
     */
    @Override
    public void getLiquidityForecastSyncState(GetLiquidityForecastSyncStateRequest request, StreamObserver<GetLiquidityForecastSyncStateResponse> observer) {
        RpcSupport.unary(observer, () -> application.getLiquidityForecastSyncState(request));
    }

    /**
     * Lists liquidity forecasts a page at a time.
     */
    @Override
    public void listLiquidityForecastConflicts(ListLiquidityForecastConflictsRequest request, StreamObserver<ListLiquidityForecastConflictsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listLiquidityForecastConflicts(request));
    }
}
