package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.BrowseChartOfAccountsesRequest;
import com.acme.accounting.v1.BrowseChartOfAccountsesResponse;
import com.acme.accounting.v1.ChartOfAccountsQueryServiceGrpc;
import com.acme.accounting.v1.CountChartOfAccountsesRequest;
import com.acme.accounting.v1.CountChartOfAccountsesResponse;
import com.acme.accounting.v1.ExportChartOfAccountsesRequest;
import com.acme.accounting.v1.ExportChartOfAccountsesResponse;
import com.acme.accounting.v1.FindChartOfAccountsesRequest;
import com.acme.accounting.v1.FindChartOfAccountsesResponse;
import com.acme.accounting.v1.LookupChartOfAccountsRequest;
import com.acme.accounting.v1.LookupChartOfAccountsResponse;
import com.acme.accounting.v1.PeekChartOfAccountsRequest;
import com.acme.accounting.v1.PeekChartOfAccountsResponse;
import com.acme.accounting.v1.ResolveChartOfAccountsesRequest;
import com.acme.accounting.v1.ResolveChartOfAccountsesResponse;
import com.acme.accounting.v1.SummarizeChartOfAccountsesRequest;
import com.acme.accounting.v1.SummarizeChartOfAccountsesResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Answers read-only questions about chart of accountses, for screens and reports.
 */
public abstract class AbstractChartOfAccountsQueryEndpoint extends ChartOfAccountsQueryServiceGrpc.ChartOfAccountsQueryServiceImplBase {
    protected final ChartOfAccountsQueryApplication application;

    protected AbstractChartOfAccountsQueryEndpoint(ChartOfAccountsQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns a chart of accounts.
     */
    @Override
    public void lookupChartOfAccounts(LookupChartOfAccountsRequest request, StreamObserver<LookupChartOfAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupChartOfAccounts(request));
    }

    /**
     * Lists chart of accountses a page at a time.
     */
    @Override
    public void browseChartOfAccountses(BrowseChartOfAccountsesRequest request, StreamObserver<BrowseChartOfAccountsesResponse> observer) {
        RpcSupport.unary(observer, () -> application.browseChartOfAccountses(request));
    }

    /**
     * Lists chart of accountses a page at a time.
     */
    @Override
    public void findChartOfAccountses(FindChartOfAccountsesRequest request, StreamObserver<FindChartOfAccountsesResponse> observer) {
        RpcSupport.unary(observer, () -> application.findChartOfAccountses(request));
    }

    /**
     * Counts chart of accountses by status.
     */
    @Override
    public void countChartOfAccountses(CountChartOfAccountsesRequest request, StreamObserver<CountChartOfAccountsesResponse> observer) {
        RpcSupport.unary(observer, () -> application.countChartOfAccountses(request));
    }

    /**
     * Returns several chart of accountses at once.
     */
    @Override
    public void resolveChartOfAccountses(ResolveChartOfAccountsesRequest request, StreamObserver<ResolveChartOfAccountsesResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveChartOfAccountses(request));
    }

    /**
     * Streams every matching chart of accounts.
     */
    @Override
    public void exportChartOfAccountses(ExportChartOfAccountsesRequest request, StreamObserver<ExportChartOfAccountsesResponse> observer) {
        try {
            var snapshot = application.exportChartOfAccountses(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Summarizes a chart of accounts.
     */
    @Override
    public void summarizeChartOfAccountses(SummarizeChartOfAccountsesRequest request, StreamObserver<SummarizeChartOfAccountsesResponse> observer) {
        RpcSupport.unary(observer, () -> application.summarizeChartOfAccountses(request));
    }

    /**
     * Returns a chart of accounts.
     */
    @Override
    public void peekChartOfAccounts(PeekChartOfAccountsRequest request, StreamObserver<PeekChartOfAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.peekChartOfAccounts(request));
    }
}
