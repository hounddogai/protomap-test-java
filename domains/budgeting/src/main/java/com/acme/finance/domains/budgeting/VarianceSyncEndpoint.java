package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.GetVarianceSyncStateRequest;
import com.acme.budgeting.v1.GetVarianceSyncStateResponse;
import com.acme.budgeting.v1.ListVarianceConflictsRequest;
import com.acme.budgeting.v1.ListVarianceConflictsResponse;
import com.acme.budgeting.v1.PullVariancesRequest;
import com.acme.budgeting.v1.PullVariancesResponse;
import com.acme.budgeting.v1.PushVarianceRequest;
import com.acme.budgeting.v1.PushVarianceResponse;
import com.acme.budgeting.v1.ReconcileVariancesRequest;
import com.acme.budgeting.v1.ReconcileVariancesResponse;
import com.acme.budgeting.v1.ResolveVarianceConflictRequest;
import com.acme.budgeting.v1.ResolveVarianceConflictResponse;
import com.acme.budgeting.v1.StreamVarianceChangesRequest;
import com.acme.budgeting.v1.StreamVarianceChangesResponse;
import com.acme.budgeting.v1.SyncVariancesRequest;
import com.acme.budgeting.v1.SyncVariancesResponse;
import com.acme.budgeting.v1.VarianceSyncServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Keeps variances in step with the systems of record in other regions.
 */
@GrpcService
public class VarianceSyncEndpoint extends VarianceSyncServiceGrpc.VarianceSyncServiceImplBase {
    protected final VarianceSyncApplication application;

    public VarianceSyncEndpoint(VarianceSyncApplication application) {
        this.application = application;
    }

    /**
     * Syncs a variance.
     */
    @Override
    public void syncVariances(SyncVariancesRequest request, StreamObserver<SyncVariancesResponse> observer) {
        RpcSupport.unary(observer, () -> application.syncVariances(request));
    }

    /**
     * Pushs a variance.
     */
    @Override
    public void pushVariance(PushVarianceRequest request, StreamObserver<PushVarianceResponse> observer) {
        RpcSupport.unary(observer, () -> application.pushVariance(request));
    }

    /**
     * Pulls a variance.
     */
    @Override
    public void pullVariances(PullVariancesRequest request, StreamObserver<PullVariancesResponse> observer) {
        RpcSupport.unary(observer, () -> application.pullVariances(request));
    }

    /**
     * Reconciles a variance.
     */
    @Override
    public void reconcileVariances(ReconcileVariancesRequest request, StreamObserver<ReconcileVariancesResponse> observer) {
        RpcSupport.unary(observer, () -> application.reconcileVariances(request));
    }

    /**
     * Exchanges changes of variances with a peer.
     */
    @Override
    public StreamObserver<StreamVarianceChangesRequest> streamVarianceChanges(StreamObserver<StreamVarianceChangesResponse> observer) {
        return new StreamObserver<StreamVarianceChangesRequest>() {
            private boolean finished;
            private final StreamVarianceChangesResponse.Builder summary = StreamVarianceChangesResponse.newBuilder();

            @Override
            public void onNext(StreamVarianceChangesRequest request) {
                if (finished) { return; }
                try {
                    var response = application.streamVarianceChanges(request);
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
     * Resolves a variance.
     */
    @Override
    public void resolveVarianceConflict(ResolveVarianceConflictRequest request, StreamObserver<ResolveVarianceConflictResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveVarianceConflict(request));
    }

    /**
     * Returns a variance.
     */
    @Override
    public void getVarianceSyncState(GetVarianceSyncStateRequest request, StreamObserver<GetVarianceSyncStateResponse> observer) {
        RpcSupport.unary(observer, () -> application.getVarianceSyncState(request));
    }

    /**
     * Lists variances a page at a time.
     */
    @Override
    public void listVarianceConflicts(ListVarianceConflictsRequest request, StreamObserver<ListVarianceConflictsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listVarianceConflicts(request));
    }
}
