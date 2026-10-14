package com.acme.finance.domains.collections;

import com.acme.collections.v1.BrowseCollectionCasesRequest;
import com.acme.collections.v1.BrowseCollectionCasesResponse;
import com.acme.collections.v1.CollectionCaseQueryServiceGrpc;
import com.acme.collections.v1.CountCollectionCasesRequest;
import com.acme.collections.v1.CountCollectionCasesResponse;
import com.acme.collections.v1.ExportCollectionCasesRequest;
import com.acme.collections.v1.ExportCollectionCasesResponse;
import com.acme.collections.v1.FindCollectionCasesRequest;
import com.acme.collections.v1.FindCollectionCasesResponse;
import com.acme.collections.v1.LookupCollectionCaseRequest;
import com.acme.collections.v1.LookupCollectionCaseResponse;
import com.acme.collections.v1.PeekCollectionCaseRequest;
import com.acme.collections.v1.PeekCollectionCaseResponse;
import com.acme.collections.v1.ResolveCollectionCasesRequest;
import com.acme.collections.v1.ResolveCollectionCasesResponse;
import com.acme.collections.v1.SuggestCollectionCasesRequest;
import com.acme.collections.v1.SuggestCollectionCasesResponse;
import com.acme.collections.v1.SummarizeCollectionCasesRequest;
import com.acme.collections.v1.SummarizeCollectionCasesResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Answers read-only questions about collection cases, for screens and reports.
 */
public abstract class AbstractCollectionCaseQueryEndpoint extends CollectionCaseQueryServiceGrpc.CollectionCaseQueryServiceImplBase {
    protected final CollectionCaseQueryApplication application;

    protected AbstractCollectionCaseQueryEndpoint(CollectionCaseQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns a collection case.
     */
    @Override
    public void lookupCollectionCase(LookupCollectionCaseRequest request, StreamObserver<LookupCollectionCaseResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupCollectionCase(request));
    }

    /**
     * Lists collection cases a page at a time.
     */
    @Override
    public void browseCollectionCases(BrowseCollectionCasesRequest request, StreamObserver<BrowseCollectionCasesResponse> observer) {
        RpcSupport.unary(observer, () -> application.browseCollectionCases(request));
    }

    /**
     * Lists collection cases a page at a time.
     */
    @Override
    public void findCollectionCases(FindCollectionCasesRequest request, StreamObserver<FindCollectionCasesResponse> observer) {
        RpcSupport.unary(observer, () -> application.findCollectionCases(request));
    }

    /**
     * Counts collection cases by status.
     */
    @Override
    public void countCollectionCases(CountCollectionCasesRequest request, StreamObserver<CountCollectionCasesResponse> observer) {
        RpcSupport.unary(observer, () -> application.countCollectionCases(request));
    }

    /**
     * Returns several collection cases at once.
     */
    @Override
    public void resolveCollectionCases(ResolveCollectionCasesRequest request, StreamObserver<ResolveCollectionCasesResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveCollectionCases(request));
    }

    /**
     * Streams every matching collection case.
     */
    @Override
    public void exportCollectionCases(ExportCollectionCasesRequest request, StreamObserver<ExportCollectionCasesResponse> observer) {
        try {
            var snapshot = application.exportCollectionCases(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Summarizes a collection case.
     */
    @Override
    public void summarizeCollectionCases(SummarizeCollectionCasesRequest request, StreamObserver<SummarizeCollectionCasesResponse> observer) {
        RpcSupport.unary(observer, () -> application.summarizeCollectionCases(request));
    }

    /**
     * Lists collection cases a page at a time.
     */
    @Override
    public void suggestCollectionCases(SuggestCollectionCasesRequest request, StreamObserver<SuggestCollectionCasesResponse> observer) {
        RpcSupport.unary(observer, () -> application.suggestCollectionCases(request));
    }

    /**
     * Returns a collection case.
     */
    @Override
    public void peekCollectionCase(PeekCollectionCaseRequest request, StreamObserver<PeekCollectionCaseResponse> observer) {
        RpcSupport.unary(observer, () -> application.peekCollectionCase(request));
    }
}
