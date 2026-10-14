package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.BrowseBudgetLinesRequest;
import com.acme.budgeting.v1.BrowseBudgetLinesResponse;
import com.acme.budgeting.v1.BudgetLineQueryServiceGrpc;
import com.acme.budgeting.v1.CountBudgetLinesRequest;
import com.acme.budgeting.v1.CountBudgetLinesResponse;
import com.acme.budgeting.v1.ExportBudgetLinesRequest;
import com.acme.budgeting.v1.ExportBudgetLinesResponse;
import com.acme.budgeting.v1.FindBudgetLinesRequest;
import com.acme.budgeting.v1.FindBudgetLinesResponse;
import com.acme.budgeting.v1.LookupBudgetLineRequest;
import com.acme.budgeting.v1.LookupBudgetLineResponse;
import com.acme.budgeting.v1.PeekBudgetLineRequest;
import com.acme.budgeting.v1.PeekBudgetLineResponse;
import com.acme.budgeting.v1.ResolveBudgetLinesRequest;
import com.acme.budgeting.v1.ResolveBudgetLinesResponse;
import com.acme.budgeting.v1.SuggestBudgetLinesRequest;
import com.acme.budgeting.v1.SuggestBudgetLinesResponse;
import com.acme.budgeting.v1.SummarizeBudgetLinesRequest;
import com.acme.budgeting.v1.SummarizeBudgetLinesResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Answers read-only questions about budget lines, for screens and reports.
 */
@GrpcService
public class BudgetLineQueryEndpoint extends BudgetLineQueryServiceGrpc.BudgetLineQueryServiceImplBase {
    protected final BudgetLineQueryApplication application;

    public BudgetLineQueryEndpoint(BudgetLineQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns a budget line.
     */
    @Override
    public void lookupBudgetLine(LookupBudgetLineRequest request, StreamObserver<LookupBudgetLineResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupBudgetLine(request));
    }

    /**
     * Lists budget lines a page at a time.
     */
    @Override
    public void browseBudgetLines(BrowseBudgetLinesRequest request, StreamObserver<BrowseBudgetLinesResponse> observer) {
        RpcSupport.unary(observer, () -> application.browseBudgetLines(request));
    }

    /**
     * Lists budget lines a page at a time.
     */
    @Override
    public void findBudgetLines(FindBudgetLinesRequest request, StreamObserver<FindBudgetLinesResponse> observer) {
        RpcSupport.unary(observer, () -> application.findBudgetLines(request));
    }

    /**
     * Counts budget lines by status.
     */
    @Override
    public void countBudgetLines(CountBudgetLinesRequest request, StreamObserver<CountBudgetLinesResponse> observer) {
        RpcSupport.unary(observer, () -> application.countBudgetLines(request));
    }

    /**
     * Returns several budget lines at once.
     */
    @Override
    public void resolveBudgetLines(ResolveBudgetLinesRequest request, StreamObserver<ResolveBudgetLinesResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveBudgetLines(request));
    }

    /**
     * Streams every matching budget line.
     */
    @Override
    public void exportBudgetLines(ExportBudgetLinesRequest request, StreamObserver<ExportBudgetLinesResponse> observer) {
        try {
            var snapshot = application.exportBudgetLines(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Summarizes a budget line.
     */
    @Override
    public void summarizeBudgetLines(SummarizeBudgetLinesRequest request, StreamObserver<SummarizeBudgetLinesResponse> observer) {
        RpcSupport.unary(observer, () -> application.summarizeBudgetLines(request));
    }

    /**
     * Lists budget lines a page at a time.
     */
    @Override
    public void suggestBudgetLines(SuggestBudgetLinesRequest request, StreamObserver<SuggestBudgetLinesResponse> observer) {
        RpcSupport.unary(observer, () -> application.suggestBudgetLines(request));
    }

    /**
     * Returns a budget line.
     */
    @Override
    public void peekBudgetLine(PeekBudgetLineRequest request, StreamObserver<PeekBudgetLineResponse> observer) {
        RpcSupport.unary(observer, () -> application.peekBudgetLine(request));
    }
}
