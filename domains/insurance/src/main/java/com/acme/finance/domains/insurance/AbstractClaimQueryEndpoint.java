package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.BrowseClaimsRequest;
import com.acme.insurance.v1.BrowseClaimsResponse;
import com.acme.insurance.v1.ClaimQueryServiceGrpc;
import com.acme.insurance.v1.CountClaimsRequest;
import com.acme.insurance.v1.CountClaimsResponse;
import com.acme.insurance.v1.ExportClaimsRequest;
import com.acme.insurance.v1.ExportClaimsResponse;
import com.acme.insurance.v1.LookupClaimRequest;
import com.acme.insurance.v1.LookupClaimResponse;
import com.acme.insurance.v1.PeekClaimRequest;
import com.acme.insurance.v1.PeekClaimResponse;
import com.acme.insurance.v1.ResolveClaimsRequest;
import com.acme.insurance.v1.ResolveClaimsResponse;
import com.acme.insurance.v1.SuggestClaimsRequest;
import com.acme.insurance.v1.SuggestClaimsResponse;
import com.acme.insurance.v1.SummarizeClaimsRequest;
import com.acme.insurance.v1.SummarizeClaimsResponse;
import io.grpc.stub.StreamObserver;

/**
 * Answers read-only questions about claims, for screens and reports.
 */
public abstract class AbstractClaimQueryEndpoint extends ClaimQueryServiceGrpc.ClaimQueryServiceImplBase {
    protected final ClaimQueryApplication application;

    protected AbstractClaimQueryEndpoint(ClaimQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns a claim.
     */
    @Override
    public void lookupClaim(LookupClaimRequest request, StreamObserver<LookupClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupClaim(request));
    }

    /**
     * Lists claims a page at a time.
     */
    @Override
    public void browseClaims(BrowseClaimsRequest request, StreamObserver<BrowseClaimsResponse> observer) {
        RpcSupport.unary(observer, () -> application.browseClaims(request));
    }

    /**
     * Counts claims by status.
     */
    @Override
    public void countClaims(CountClaimsRequest request, StreamObserver<CountClaimsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countClaims(request));
    }

    /**
     * Returns several claims at once.
     */
    @Override
    public void resolveClaims(ResolveClaimsRequest request, StreamObserver<ResolveClaimsResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveClaims(request));
    }

    /**
     * Streams every matching claim.
     */
    @Override
    public void exportClaims(ExportClaimsRequest request, StreamObserver<ExportClaimsResponse> observer) {
        try {
            var snapshot = application.exportClaims(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Summarizes a claim.
     */
    @Override
    public void summarizeClaims(SummarizeClaimsRequest request, StreamObserver<SummarizeClaimsResponse> observer) {
        RpcSupport.unary(observer, () -> application.summarizeClaims(request));
    }

    /**
     * Lists claims a page at a time.
     */
    @Override
    public void suggestClaims(SuggestClaimsRequest request, StreamObserver<SuggestClaimsResponse> observer) {
        RpcSupport.unary(observer, () -> application.suggestClaims(request));
    }

    /**
     * Returns a claim.
     */
    @Override
    public void peekClaim(PeekClaimRequest request, StreamObserver<PeekClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.peekClaim(request));
    }
}
