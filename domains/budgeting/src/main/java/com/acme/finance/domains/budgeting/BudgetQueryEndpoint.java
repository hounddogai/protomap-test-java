package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.BrowseBudgetsRequest;
import com.acme.budgeting.v1.BrowseBudgetsResponse;
import com.acme.budgeting.v1.BudgetQueryServiceGrpc;
import com.acme.budgeting.v1.CountBudgetsRequest;
import com.acme.budgeting.v1.CountBudgetsResponse;
import com.acme.budgeting.v1.ExportBudgetsRequest;
import com.acme.budgeting.v1.ExportBudgetsResponse;
import com.acme.budgeting.v1.FindBudgetsRequest;
import com.acme.budgeting.v1.FindBudgetsResponse;
import com.acme.budgeting.v1.LookupBudgetRequest;
import com.acme.budgeting.v1.LookupBudgetResponse;
import com.acme.budgeting.v1.PeekBudgetRequest;
import com.acme.budgeting.v1.PeekBudgetResponse;
import com.acme.budgeting.v1.ResolveBudgetsRequest;
import com.acme.budgeting.v1.ResolveBudgetsResponse;
import com.acme.budgeting.v1.SuggestBudgetsRequest;
import com.acme.budgeting.v1.SuggestBudgetsResponse;
import com.acme.budgeting.v1.SummarizeBudgetsRequest;
import com.acme.budgeting.v1.SummarizeBudgetsResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Answers read-only questions about budgets, for screens and reports.
 */
@GrpcService
public class BudgetQueryEndpoint extends BudgetQueryServiceGrpc.BudgetQueryServiceImplBase {
    protected final BudgetQueryApplication application;

    public BudgetQueryEndpoint(BudgetQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns a budget.
     */
    @Override
    public void lookupBudget(LookupBudgetRequest request, StreamObserver<LookupBudgetResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupBudget(request));
    }

    /**
     * Lists budgets a page at a time.
     */
    @Override
    public void browseBudgets(BrowseBudgetsRequest request, StreamObserver<BrowseBudgetsResponse> observer) {
        RpcSupport.unary(observer, () -> application.browseBudgets(request));
    }

    /**
     * Lists budgets a page at a time.
     */
    @Override
    public void findBudgets(FindBudgetsRequest request, StreamObserver<FindBudgetsResponse> observer) {
        RpcSupport.unary(observer, () -> application.findBudgets(request));
    }

    /**
     * Counts budgets by status.
     */
    @Override
    public void countBudgets(CountBudgetsRequest request, StreamObserver<CountBudgetsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countBudgets(request));
    }

    /**
     * Returns several budgets at once.
     */
    @Override
    public void resolveBudgets(ResolveBudgetsRequest request, StreamObserver<ResolveBudgetsResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveBudgets(request));
    }

    /**
     * Streams every matching budget.
     */
    @Override
    public void exportBudgets(ExportBudgetsRequest request, StreamObserver<ExportBudgetsResponse> observer) {
        try {
            var snapshot = application.exportBudgets(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Summarizes a budget.
     */
    @Override
    public void summarizeBudgets(SummarizeBudgetsRequest request, StreamObserver<SummarizeBudgetsResponse> observer) {
        RpcSupport.unary(observer, () -> application.summarizeBudgets(request));
    }

    /**
     * Lists budgets a page at a time.
     */
    @Override
    public void suggestBudgets(SuggestBudgetsRequest request, StreamObserver<SuggestBudgetsResponse> observer) {
        RpcSupport.unary(observer, () -> application.suggestBudgets(request));
    }

    /**
     * Returns a budget.
     */
    @Override
    public void peekBudget(PeekBudgetRequest request, StreamObserver<PeekBudgetResponse> observer) {
        RpcSupport.unary(observer, () -> application.peekBudget(request));
    }
}
