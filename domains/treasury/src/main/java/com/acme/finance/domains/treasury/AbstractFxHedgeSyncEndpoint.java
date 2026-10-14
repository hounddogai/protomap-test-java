package com.acme.finance.domains.treasury;

import com.acme.finance.platform.RpcSupport;
import com.acme.treasury.v1.FxHedgeSyncServiceGrpc;
import com.acme.treasury.v1.GetFxHedgeSyncStateRequest;
import com.acme.treasury.v1.GetFxHedgeSyncStateResponse;
import com.acme.treasury.v1.ListFxHedgeConflictsRequest;
import com.acme.treasury.v1.ListFxHedgeConflictsResponse;
import com.acme.treasury.v1.PushFxHedgeRequest;
import com.acme.treasury.v1.PushFxHedgeResponse;
import com.acme.treasury.v1.ReconcileFxHedgesRequest;
import com.acme.treasury.v1.ReconcileFxHedgesResponse;
import com.acme.treasury.v1.ResolveFxHedgeConflictRequest;
import com.acme.treasury.v1.ResolveFxHedgeConflictResponse;
import com.acme.treasury.v1.StreamFxHedgeChangesRequest;
import com.acme.treasury.v1.StreamFxHedgeChangesResponse;
import com.acme.treasury.v1.SyncFxHedgesRequest;
import com.acme.treasury.v1.SyncFxHedgesResponse;
import io.grpc.stub.StreamObserver;

/**
 * Keeps fx hedges in step with the systems of record in other regions.
 */
public abstract class AbstractFxHedgeSyncEndpoint extends FxHedgeSyncServiceGrpc.FxHedgeSyncServiceImplBase {
    protected final FxHedgeSyncApplication application;

    protected AbstractFxHedgeSyncEndpoint(FxHedgeSyncApplication application) {
        this.application = application;
    }

    /**
     * Syncs a fx hedge.
     */
    @Override
    public void syncFxHedges(SyncFxHedgesRequest request, StreamObserver<SyncFxHedgesResponse> observer) {
        RpcSupport.unary(observer, () -> application.syncFxHedges(request));
    }

    /**
     * Pushs a fx hedge.
     */
    @Override
    public void pushFxHedge(PushFxHedgeRequest request, StreamObserver<PushFxHedgeResponse> observer) {
        RpcSupport.unary(observer, () -> application.pushFxHedge(request));
    }

    /**
     * Reconciles a fx hedge.
     */
    @Override
    public void reconcileFxHedges(ReconcileFxHedgesRequest request, StreamObserver<ReconcileFxHedgesResponse> observer) {
        RpcSupport.unary(observer, () -> application.reconcileFxHedges(request));
    }

    /**
     * Exchanges changes of fx hedges with a peer.
     */
    @Override
    public StreamObserver<StreamFxHedgeChangesRequest> streamFxHedgeChanges(StreamObserver<StreamFxHedgeChangesResponse> observer) {
        return new StreamObserver<StreamFxHedgeChangesRequest>() {
            private boolean finished;
            private final StreamFxHedgeChangesResponse.Builder summary = StreamFxHedgeChangesResponse.newBuilder();

            @Override
            public void onNext(StreamFxHedgeChangesRequest request) {
                if (finished) { return; }
                try {
                    var response = application.streamFxHedgeChanges(request);
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
     * Resolves a fx hedge.
     */
    @Override
    public void resolveFxHedgeConflict(ResolveFxHedgeConflictRequest request, StreamObserver<ResolveFxHedgeConflictResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveFxHedgeConflict(request));
    }

    /**
     * Returns a fx hedge.
     */
    @Override
    public void getFxHedgeSyncState(GetFxHedgeSyncStateRequest request, StreamObserver<GetFxHedgeSyncStateResponse> observer) {
        RpcSupport.unary(observer, () -> application.getFxHedgeSyncState(request));
    }

    /**
     * Lists fx hedges a page at a time.
     */
    @Override
    public void listFxHedgeConflicts(ListFxHedgeConflictsRequest request, StreamObserver<ListFxHedgeConflictsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listFxHedgeConflicts(request));
    }
}
