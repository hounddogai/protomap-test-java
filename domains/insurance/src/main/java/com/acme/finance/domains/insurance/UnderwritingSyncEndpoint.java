package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.GetUnderwritingSyncStateRequest;
import com.acme.insurance.v1.GetUnderwritingSyncStateResponse;
import com.acme.insurance.v1.ListUnderwritingConflictsRequest;
import com.acme.insurance.v1.ListUnderwritingConflictsResponse;
import com.acme.insurance.v1.PullUnderwritingsRequest;
import com.acme.insurance.v1.PullUnderwritingsResponse;
import com.acme.insurance.v1.PushUnderwritingRequest;
import com.acme.insurance.v1.PushUnderwritingResponse;
import com.acme.insurance.v1.ReconcileUnderwritingsRequest;
import com.acme.insurance.v1.ReconcileUnderwritingsResponse;
import com.acme.insurance.v1.ResolveUnderwritingConflictRequest;
import com.acme.insurance.v1.ResolveUnderwritingConflictResponse;
import com.acme.insurance.v1.StreamUnderwritingChangesRequest;
import com.acme.insurance.v1.StreamUnderwritingChangesResponse;
import com.acme.insurance.v1.SyncUnderwritingsRequest;
import com.acme.insurance.v1.SyncUnderwritingsResponse;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Keeps underwritings in step with the systems of record in other regions.
 */
@GrpcService
public class UnderwritingSyncEndpoint extends UnderwritingSyncServiceGrpc.UnderwritingSyncServiceImplBase {
    protected final UnderwritingSyncApplication application;

    public UnderwritingSyncEndpoint(UnderwritingSyncApplication application) {
        this.application = application;
    }

    /**
     * Syncs an underwriting.
     */
    @Override
    public void syncUnderwritings(SyncUnderwritingsRequest request, StreamObserver<SyncUnderwritingsResponse> observer) {
        RpcSupport.unary(observer, () -> application.syncUnderwritings(request));
    }

    /**
     * Pushs an underwriting.
     */
    @Override
    public void pushUnderwriting(PushUnderwritingRequest request, StreamObserver<PushUnderwritingResponse> observer) {
        RpcSupport.unary(observer, () -> application.pushUnderwriting(request));
    }

    /**
     * Pulls an underwriting.
     */
    @Override
    public void pullUnderwritings(PullUnderwritingsRequest request, StreamObserver<PullUnderwritingsResponse> observer) {
        RpcSupport.unary(observer, () -> application.pullUnderwritings(request));
    }

    /**
     * Reconciles an underwriting.
     */
    @Override
    public void reconcileUnderwritings(ReconcileUnderwritingsRequest request, StreamObserver<ReconcileUnderwritingsResponse> observer) {
        RpcSupport.unary(observer, () -> application.reconcileUnderwritings(request));
    }

    /**
     * Exchanges changes of underwritings with a peer.
     */
    @Override
    public StreamObserver<StreamUnderwritingChangesRequest> streamUnderwritingChanges(StreamObserver<StreamUnderwritingChangesResponse> observer) {
        return new StreamObserver<StreamUnderwritingChangesRequest>() {
            private boolean finished;
            private final StreamUnderwritingChangesResponse.Builder summary = StreamUnderwritingChangesResponse.newBuilder();

            @Override
            public void onNext(StreamUnderwritingChangesRequest request) {
                if (finished) { return; }
                try {
                    var response = application.streamUnderwritingChanges(request);
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
     * Resolves an underwriting.
     */
    @Override
    public void resolveUnderwritingConflict(ResolveUnderwritingConflictRequest request, StreamObserver<ResolveUnderwritingConflictResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveUnderwritingConflict(request));
    }

    /**
     * Returns an underwriting.
     */
    @Override
    public void getUnderwritingSyncState(GetUnderwritingSyncStateRequest request, StreamObserver<GetUnderwritingSyncStateResponse> observer) {
        RpcSupport.unary(observer, () -> application.getUnderwritingSyncState(request));
    }

    /**
     * Lists underwritings a page at a time.
     */
    @Override
    public void listUnderwritingConflicts(ListUnderwritingConflictsRequest request, StreamObserver<ListUnderwritingConflictsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listUnderwritingConflicts(request));
    }
}
