package com.acme.finance.domains.expenses;

import com.acme.expenses.v1.ExpensePolicySyncServiceGrpc;
import com.acme.expenses.v1.GetExpensePolicySyncStateRequest;
import com.acme.expenses.v1.GetExpensePolicySyncStateResponse;
import com.acme.expenses.v1.ListExpensePolicyConflictsRequest;
import com.acme.expenses.v1.ListExpensePolicyConflictsResponse;
import com.acme.expenses.v1.PullExpensePoliciesRequest;
import com.acme.expenses.v1.PullExpensePoliciesResponse;
import com.acme.expenses.v1.PushExpensePolicyRequest;
import com.acme.expenses.v1.PushExpensePolicyResponse;
import com.acme.expenses.v1.ReconcileExpensePoliciesRequest;
import com.acme.expenses.v1.ReconcileExpensePoliciesResponse;
import com.acme.expenses.v1.ResolveExpensePolicyConflictRequest;
import com.acme.expenses.v1.ResolveExpensePolicyConflictResponse;
import com.acme.expenses.v1.StreamExpensePolicyChangesRequest;
import com.acme.expenses.v1.StreamExpensePolicyChangesResponse;
import com.acme.expenses.v1.SyncExpensePoliciesRequest;
import com.acme.expenses.v1.SyncExpensePoliciesResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Keeps expense policies in step with the systems of record in other regions.
 */
@GrpcService
public class ExpensePolicySyncEndpoint extends ExpensePolicySyncServiceGrpc.ExpensePolicySyncServiceImplBase {
    protected final ExpensePolicySyncApplication application;

    public ExpensePolicySyncEndpoint(ExpensePolicySyncApplication application) {
        this.application = application;
    }

    /**
     * Syncs an expense policy.
     */
    @Override
    public void syncExpensePolicies(SyncExpensePoliciesRequest request, StreamObserver<SyncExpensePoliciesResponse> observer) {
        RpcSupport.unary(observer, () -> application.syncExpensePolicies(request));
    }

    /**
     * Pushs an expense policy.
     */
    @Override
    public void pushExpensePolicy(PushExpensePolicyRequest request, StreamObserver<PushExpensePolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.pushExpensePolicy(request));
    }

    /**
     * Pulls an expense policy.
     */
    @Override
    public void pullExpensePolicies(PullExpensePoliciesRequest request, StreamObserver<PullExpensePoliciesResponse> observer) {
        RpcSupport.unary(observer, () -> application.pullExpensePolicies(request));
    }

    /**
     * Reconciles an expense policy.
     */
    @Override
    public void reconcileExpensePolicies(ReconcileExpensePoliciesRequest request, StreamObserver<ReconcileExpensePoliciesResponse> observer) {
        RpcSupport.unary(observer, () -> application.reconcileExpensePolicies(request));
    }

    /**
     * Exchanges changes of expense policies with a peer.
     */
    @Override
    public StreamObserver<StreamExpensePolicyChangesRequest> streamExpensePolicyChanges(StreamObserver<StreamExpensePolicyChangesResponse> observer) {
        return new StreamObserver<StreamExpensePolicyChangesRequest>() {
            private boolean finished;
            private final StreamExpensePolicyChangesResponse.Builder summary = StreamExpensePolicyChangesResponse.newBuilder();

            @Override
            public void onNext(StreamExpensePolicyChangesRequest request) {
                if (finished) { return; }
                try {
                    var response = application.streamExpensePolicyChanges(request);
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
     * Resolves an expense policy.
     */
    @Override
    public void resolveExpensePolicyConflict(ResolveExpensePolicyConflictRequest request, StreamObserver<ResolveExpensePolicyConflictResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveExpensePolicyConflict(request));
    }

    /**
     * Returns an expense policy.
     */
    @Override
    public void getExpensePolicySyncState(GetExpensePolicySyncStateRequest request, StreamObserver<GetExpensePolicySyncStateResponse> observer) {
        RpcSupport.unary(observer, () -> application.getExpensePolicySyncState(request));
    }

    /**
     * Lists expense policies a page at a time.
     */
    @Override
    public void listExpensePolicyConflicts(ListExpensePolicyConflictsRequest request, StreamObserver<ListExpensePolicyConflictsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listExpensePolicyConflicts(request));
    }
}
