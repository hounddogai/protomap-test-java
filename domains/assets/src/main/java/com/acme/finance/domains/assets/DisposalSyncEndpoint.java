package com.acme.finance.domains.assets;

import com.acme.assets.v1.DisposalSyncServiceGrpc;
import com.acme.assets.v1.GetDisposalSyncStateRequest;
import com.acme.assets.v1.GetDisposalSyncStateResponse;
import com.acme.assets.v1.ListDisposalConflictsRequest;
import com.acme.assets.v1.ListDisposalConflictsResponse;
import com.acme.assets.v1.PushDisposalRequest;
import com.acme.assets.v1.PushDisposalResponse;
import com.acme.assets.v1.ReconcileDisposalsRequest;
import com.acme.assets.v1.ReconcileDisposalsResponse;
import com.acme.assets.v1.StreamDisposalChangesRequest;
import com.acme.assets.v1.StreamDisposalChangesResponse;
import com.acme.assets.v1.SyncDisposalsRequest;
import com.acme.assets.v1.SyncDisposalsResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Keeps disposals in step with the systems of record in other regions.
 */
@GrpcService
public class DisposalSyncEndpoint extends DisposalSyncServiceGrpc.DisposalSyncServiceImplBase {
    protected final DisposalSyncApplication application;

    public DisposalSyncEndpoint(DisposalSyncApplication application) {
        this.application = application;
    }

    /**
     * Syncs a disposal.
     */
    @Override
    public void syncDisposals(SyncDisposalsRequest request, StreamObserver<SyncDisposalsResponse> observer) {
        RpcSupport.unary(observer, () -> application.syncDisposals(request));
    }

    /**
     * Pushs a disposal.
     */
    @Override
    public void pushDisposal(PushDisposalRequest request, StreamObserver<PushDisposalResponse> observer) {
        RpcSupport.unary(observer, () -> application.pushDisposal(request));
    }

    /**
     * Reconciles a disposal.
     */
    @Override
    public void reconcileDisposals(ReconcileDisposalsRequest request, StreamObserver<ReconcileDisposalsResponse> observer) {
        RpcSupport.unary(observer, () -> application.reconcileDisposals(request));
    }

    /**
     * Exchanges changes of disposals with a peer.
     */
    @Override
    public StreamObserver<StreamDisposalChangesRequest> streamDisposalChanges(StreamObserver<StreamDisposalChangesResponse> observer) {
        return new StreamObserver<StreamDisposalChangesRequest>() {
            private boolean finished;
            private final StreamDisposalChangesResponse.Builder summary = StreamDisposalChangesResponse.newBuilder();

            @Override
            public void onNext(StreamDisposalChangesRequest request) {
                if (finished) { return; }
                try {
                    var response = application.streamDisposalChanges(request);
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
     * Returns a disposal.
     */
    @Override
    public void getDisposalSyncState(GetDisposalSyncStateRequest request, StreamObserver<GetDisposalSyncStateResponse> observer) {
        RpcSupport.unary(observer, () -> application.getDisposalSyncState(request));
    }

    /**
     * Lists disposals a page at a time.
     */
    @Override
    public void listDisposalConflicts(ListDisposalConflictsRequest request, StreamObserver<ListDisposalConflictsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listDisposalConflicts(request));
    }
}
