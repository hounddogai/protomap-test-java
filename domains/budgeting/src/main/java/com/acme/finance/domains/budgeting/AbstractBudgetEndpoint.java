package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.BatchCreateBudgetsRequest;
import com.acme.budgeting.v1.BatchCreateBudgetsResponse;
import com.acme.budgeting.v1.BatchGetBudgetsRequest;
import com.acme.budgeting.v1.BatchGetBudgetsResponse;
import com.acme.budgeting.v1.BudgetServiceGrpc;
import com.acme.budgeting.v1.CreateBudgetRequest;
import com.acme.budgeting.v1.CreateBudgetResponse;
import com.acme.budgeting.v1.DeleteBudgetRequest;
import com.acme.budgeting.v1.DeleteBudgetResponse;
import com.acme.budgeting.v1.GetBudgetRequest;
import com.acme.budgeting.v1.GetBudgetResponse;
import com.acme.budgeting.v1.ListBudgetsRequest;
import com.acme.budgeting.v1.ListBudgetsResponse;
import com.acme.budgeting.v1.UpdateBudgetRequest;
import com.acme.budgeting.v1.UpdateBudgetResponse;
import com.acme.budgeting.v1.WatchBudgetsRequest;
import com.acme.budgeting.v1.WatchBudgetsResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Manages budgets: creates, reads, changes, and lists them.
 */
public abstract class AbstractBudgetEndpoint extends BudgetServiceGrpc.BudgetServiceImplBase {
    protected final BudgetApplication application;

    protected AbstractBudgetEndpoint(BudgetApplication application) {
        this.application = application;
    }

    /**
     * Creates a budget.
     */
    @Override
    public void createBudget(CreateBudgetRequest request, StreamObserver<CreateBudgetResponse> observer) {
        RpcSupport.unary(observer, () -> application.createBudget(request));
    }

    /**
     * Returns a budget.
     */
    @Override
    public void getBudget(GetBudgetRequest request, StreamObserver<GetBudgetResponse> observer) {
        RpcSupport.unary(observer, () -> application.getBudget(request));
    }

    /**
     * Changes a budget.
     */
    @Override
    public void updateBudget(UpdateBudgetRequest request, StreamObserver<UpdateBudgetResponse> observer) {
        RpcSupport.unary(observer, () -> application.updateBudget(request));
    }

    /**
     * Deletes a budget.
     */
    @Override
    public void deleteBudget(DeleteBudgetRequest request, StreamObserver<DeleteBudgetResponse> observer) {
        RpcSupport.unary(observer, () -> application.deleteBudget(request));
    }

    /**
     * Lists budgets a page at a time.
     */
    @Override
    public void listBudgets(ListBudgetsRequest request, StreamObserver<ListBudgetsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listBudgets(request));
    }

    /**
     * Returns several budgets at once.
     */
    @Override
    public void batchGetBudgets(BatchGetBudgetsRequest request, StreamObserver<BatchGetBudgetsResponse> observer) {
        RpcSupport.unary(observer, () -> application.batchGetBudgets(request));
    }

    /**
     * Streams the changes of budgets as they happen.
     */
    @Override
    public void watchBudgets(WatchBudgetsRequest request, StreamObserver<WatchBudgetsResponse> observer) {
        try {
            var snapshot = application.watchBudgets(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Takes a stream of budgets and answers once at the end.
     */
    @Override
    public StreamObserver<BatchCreateBudgetsRequest> batchCreateBudgets(StreamObserver<BatchCreateBudgetsResponse> observer) {
        return new StreamObserver<BatchCreateBudgetsRequest>() {
            private boolean finished;
            private final BatchCreateBudgetsResponse.Builder summary = BatchCreateBudgetsResponse.newBuilder();

            @Override
            public void onNext(BatchCreateBudgetsRequest request) {
                if (finished) { return; }
                try {
                    var response = application.batchCreateBudgets(request);
                    summary.mergeFrom(response);
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
                observer.onNext(summary.build());
                observer.onCompleted();
            }
        };
    }
}
