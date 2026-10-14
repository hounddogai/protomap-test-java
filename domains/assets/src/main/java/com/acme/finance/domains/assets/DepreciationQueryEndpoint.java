package com.acme.finance.domains.assets;

import com.acme.assets.v1.BrowseDepreciationsRequest;
import com.acme.assets.v1.BrowseDepreciationsResponse;
import com.acme.assets.v1.CountDepreciationsRequest;
import com.acme.assets.v1.CountDepreciationsResponse;
import com.acme.assets.v1.DepreciationQueryServiceGrpc;
import com.acme.assets.v1.ExportDepreciationsRequest;
import com.acme.assets.v1.ExportDepreciationsResponse;
import com.acme.assets.v1.FindDepreciationsRequest;
import com.acme.assets.v1.FindDepreciationsResponse;
import com.acme.assets.v1.LookupDepreciationRequest;
import com.acme.assets.v1.LookupDepreciationResponse;
import com.acme.assets.v1.PeekDepreciationRequest;
import com.acme.assets.v1.PeekDepreciationResponse;
import com.acme.assets.v1.ResolveDepreciationsRequest;
import com.acme.assets.v1.ResolveDepreciationsResponse;
import com.acme.assets.v1.SuggestDepreciationsRequest;
import com.acme.assets.v1.SuggestDepreciationsResponse;
import com.acme.assets.v1.SummarizeDepreciationsRequest;
import com.acme.assets.v1.SummarizeDepreciationsResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Answers read-only questions about depreciations, for screens and reports.
 */
@GrpcService
public class DepreciationQueryEndpoint extends DepreciationQueryServiceGrpc.DepreciationQueryServiceImplBase {
    protected final DepreciationQueryApplication application;

    public DepreciationQueryEndpoint(DepreciationQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns a depreciation.
     */
    @Override
    public void lookupDepreciation(LookupDepreciationRequest request, StreamObserver<LookupDepreciationResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupDepreciation(request));
    }

    /**
     * Lists depreciations a page at a time.
     */
    @Override
    public void browseDepreciations(BrowseDepreciationsRequest request, StreamObserver<BrowseDepreciationsResponse> observer) {
        RpcSupport.unary(observer, () -> application.browseDepreciations(request));
    }

    /**
     * Lists depreciations a page at a time.
     */
    @Override
    public void findDepreciations(FindDepreciationsRequest request, StreamObserver<FindDepreciationsResponse> observer) {
        RpcSupport.unary(observer, () -> application.findDepreciations(request));
    }

    /**
     * Counts depreciations by status.
     */
    @Override
    public void countDepreciations(CountDepreciationsRequest request, StreamObserver<CountDepreciationsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countDepreciations(request));
    }

    /**
     * Returns several depreciations at once.
     */
    @Override
    public void resolveDepreciations(ResolveDepreciationsRequest request, StreamObserver<ResolveDepreciationsResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveDepreciations(request));
    }

    /**
     * Streams every matching depreciation.
     */
    @Override
    public void exportDepreciations(ExportDepreciationsRequest request, StreamObserver<ExportDepreciationsResponse> observer) {
        try {
            var snapshot = application.exportDepreciations(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Summarizes a depreciation.
     */
    @Override
    public void summarizeDepreciations(SummarizeDepreciationsRequest request, StreamObserver<SummarizeDepreciationsResponse> observer) {
        RpcSupport.unary(observer, () -> application.summarizeDepreciations(request));
    }

    /**
     * Lists depreciations a page at a time.
     */
    @Override
    public void suggestDepreciations(SuggestDepreciationsRequest request, StreamObserver<SuggestDepreciationsResponse> observer) {
        RpcSupport.unary(observer, () -> application.suggestDepreciations(request));
    }

    /**
     * Returns a depreciation.
     */
    @Override
    public void peekDepreciation(PeekDepreciationRequest request, StreamObserver<PeekDepreciationResponse> observer) {
        RpcSupport.unary(observer, () -> application.peekDepreciation(request));
    }
}
