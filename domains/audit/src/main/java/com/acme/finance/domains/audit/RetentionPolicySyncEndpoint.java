package com.acme.finance.domains.audit;

import com.acme.audit.v1.GetRetentionPolicySyncStateRequest;
import com.acme.audit.v1.GetRetentionPolicySyncStateResponse;
import com.acme.audit.v1.ListRetentionPolicyConflictsRequest;
import com.acme.audit.v1.ListRetentionPolicyConflictsResponse;
import com.acme.audit.v1.PullRetentionPoliciesRequest;
import com.acme.audit.v1.PullRetentionPoliciesResponse;
import com.acme.audit.v1.PushRetentionPolicyRequest;
import com.acme.audit.v1.PushRetentionPolicyResponse;
import com.acme.audit.v1.ReconcileRetentionPoliciesRequest;
import com.acme.audit.v1.ReconcileRetentionPoliciesResponse;
import com.acme.audit.v1.ResolveRetentionPolicyConflictRequest;
import com.acme.audit.v1.ResolveRetentionPolicyConflictResponse;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.SyncRetentionPoliciesRequest;
import com.acme.audit.v1.SyncRetentionPoliciesResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Keeps retention policies in step with the systems of record in other regions.
 */
@GrpcService
public class RetentionPolicySyncEndpoint extends RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceImplBase {
    protected final RetentionPolicySyncApplication application;

    public RetentionPolicySyncEndpoint(RetentionPolicySyncApplication application) {
        this.application = application;
    }

    /**
     * Syncs a retention policy.
     */
    @Override
    public void syncRetentionPolicies(SyncRetentionPoliciesRequest request, StreamObserver<SyncRetentionPoliciesResponse> observer) {
        RpcSupport.unary(observer, () -> application.syncRetentionPolicies(request));
    }

    /**
     * Pushs a retention policy.
     */
    @Override
    public void pushRetentionPolicy(PushRetentionPolicyRequest request, StreamObserver<PushRetentionPolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.pushRetentionPolicy(request));
    }

    /**
     * Pulls a retention policy.
     */
    @Override
    public void pullRetentionPolicies(PullRetentionPoliciesRequest request, StreamObserver<PullRetentionPoliciesResponse> observer) {
        RpcSupport.unary(observer, () -> application.pullRetentionPolicies(request));
    }

    /**
     * Reconciles a retention policy.
     */
    @Override
    public void reconcileRetentionPolicies(ReconcileRetentionPoliciesRequest request, StreamObserver<ReconcileRetentionPoliciesResponse> observer) {
        RpcSupport.unary(observer, () -> application.reconcileRetentionPolicies(request));
    }

    /**
     * Exchanges changes of retention policies with a peer.
     */
    @Override
    public StreamObserver<StreamRetentionPolicyChangesRequest> streamRetentionPolicyChanges(StreamObserver<StreamRetentionPolicyChangesResponse> observer) {
        return new StreamObserver<StreamRetentionPolicyChangesRequest>() {
            private boolean finished;
            private final StreamRetentionPolicyChangesResponse.Builder summary = StreamRetentionPolicyChangesResponse.newBuilder();

            @Override
            public void onNext(StreamRetentionPolicyChangesRequest request) {
                if (finished) { return; }
                try {
                    var response = application.streamRetentionPolicyChanges(request);
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
     * Resolves a retention policy.
     */
    @Override
    public void resolveRetentionPolicyConflict(ResolveRetentionPolicyConflictRequest request, StreamObserver<ResolveRetentionPolicyConflictResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveRetentionPolicyConflict(request));
    }

    /**
     * Returns a retention policy.
     */
    @Override
    public void getRetentionPolicySyncState(GetRetentionPolicySyncStateRequest request, StreamObserver<GetRetentionPolicySyncStateResponse> observer) {
        RpcSupport.unary(observer, () -> application.getRetentionPolicySyncState(request));
    }

    /**
     * Lists retention policies a page at a time.
     */
    @Override
    public void listRetentionPolicyConflicts(ListRetentionPolicyConflictsRequest request, StreamObserver<ListRetentionPolicyConflictsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listRetentionPolicyConflicts(request));
    }
}
