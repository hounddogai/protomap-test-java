package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.ListPremiumConflictsRequest;
import com.acme.insurance.v1.ListPremiumConflictsResponse;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.PullPremiumsRequest;
import com.acme.insurance.v1.PullPremiumsResponse;
import com.acme.insurance.v1.PushPremiumRequest;
import com.acme.insurance.v1.PushPremiumResponse;
import com.acme.insurance.v1.ResolvePremiumConflictRequest;
import com.acme.insurance.v1.ResolvePremiumConflictResponse;
import com.acme.insurance.v1.StreamPremiumChangesRequest;
import com.acme.insurance.v1.StreamPremiumChangesResponse;
import com.acme.insurance.v1.SyncPremiumsRequest;
import com.acme.insurance.v1.SyncPremiumsResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Keeps premiums in step with the systems of record in other regions.
 */
@GrpcService
public class PremiumSyncEndpoint extends PremiumSyncServiceGrpc.PremiumSyncServiceImplBase {
    protected final PremiumSyncApplication application;

    public PremiumSyncEndpoint(PremiumSyncApplication application) {
        this.application = application;
    }

    /**
     * Syncs a premium.
     */
    @Override
    public void syncPremiums(SyncPremiumsRequest request, StreamObserver<SyncPremiumsResponse> observer) {
        RpcSupport.unary(observer, () -> application.syncPremiums(request));
    }

    /**
     * Pushs a premium.
     */
    @Override
    public void pushPremium(PushPremiumRequest request, StreamObserver<PushPremiumResponse> observer) {
        RpcSupport.unary(observer, () -> application.pushPremium(request));
    }

    /**
     * Pulls a premium.
     */
    @Override
    public void pullPremiums(PullPremiumsRequest request, StreamObserver<PullPremiumsResponse> observer) {
        RpcSupport.unary(observer, () -> application.pullPremiums(request));
    }

    /**
     * Exchanges changes of premiums with a peer.
     */
    @Override
    public StreamObserver<StreamPremiumChangesRequest> streamPremiumChanges(StreamObserver<StreamPremiumChangesResponse> observer) {
        return new StreamObserver<StreamPremiumChangesRequest>() {
            private boolean finished;
            private final StreamPremiumChangesResponse.Builder summary = StreamPremiumChangesResponse.newBuilder();

            @Override
            public void onNext(StreamPremiumChangesRequest request) {
                if (finished) { return; }
                try {
                    var response = application.streamPremiumChanges(request);
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
     * Resolves a premium.
     */
    @Override
    public void resolvePremiumConflict(ResolvePremiumConflictRequest request, StreamObserver<ResolvePremiumConflictResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolvePremiumConflict(request));
    }

    /**
     * Lists premiums a page at a time.
     */
    @Override
    public void listPremiumConflicts(ListPremiumConflictsRequest request, StreamObserver<ListPremiumConflictsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPremiumConflicts(request));
    }
}
