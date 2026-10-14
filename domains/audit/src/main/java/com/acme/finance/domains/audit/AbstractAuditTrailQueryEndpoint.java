package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditTrailQueryServiceGrpc;
import com.acme.audit.v1.BrowseAuditTrailsRequest;
import com.acme.audit.v1.BrowseAuditTrailsResponse;
import com.acme.audit.v1.ExportAuditTrailsRequest;
import com.acme.audit.v1.ExportAuditTrailsResponse;
import com.acme.audit.v1.FindAuditTrailsRequest;
import com.acme.audit.v1.FindAuditTrailsResponse;
import com.acme.audit.v1.LookupAuditTrailRequest;
import com.acme.audit.v1.LookupAuditTrailResponse;
import com.acme.audit.v1.ResolveAuditTrailsRequest;
import com.acme.audit.v1.ResolveAuditTrailsResponse;
import com.acme.audit.v1.SuggestAuditTrailsRequest;
import com.acme.audit.v1.SuggestAuditTrailsResponse;
import com.acme.audit.v1.SummarizeAuditTrailsRequest;
import com.acme.audit.v1.SummarizeAuditTrailsResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Answers read-only questions about audit trails, for screens and reports.
 */
public abstract class AbstractAuditTrailQueryEndpoint extends AuditTrailQueryServiceGrpc.AuditTrailQueryServiceImplBase {
    protected final AuditTrailQueryApplication application;

    protected AbstractAuditTrailQueryEndpoint(AuditTrailQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns an audit trail.
     */
    @Override
    public void lookupAuditTrail(LookupAuditTrailRequest request, StreamObserver<LookupAuditTrailResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupAuditTrail(request));
    }

    /**
     * Lists audit trails a page at a time.
     */
    @Override
    public void browseAuditTrails(BrowseAuditTrailsRequest request, StreamObserver<BrowseAuditTrailsResponse> observer) {
        RpcSupport.unary(observer, () -> application.browseAuditTrails(request));
    }

    /**
     * Lists audit trails a page at a time.
     */
    @Override
    public void findAuditTrails(FindAuditTrailsRequest request, StreamObserver<FindAuditTrailsResponse> observer) {
        RpcSupport.unary(observer, () -> application.findAuditTrails(request));
    }

    /**
     * Returns several audit trails at once.
     */
    @Override
    public void resolveAuditTrails(ResolveAuditTrailsRequest request, StreamObserver<ResolveAuditTrailsResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveAuditTrails(request));
    }

    /**
     * Streams every matching audit trail.
     */
    @Override
    public void exportAuditTrails(ExportAuditTrailsRequest request, StreamObserver<ExportAuditTrailsResponse> observer) {
        try {
            var snapshot = application.exportAuditTrails(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Summarizes an audit trail.
     */
    @Override
    public void summarizeAuditTrails(SummarizeAuditTrailsRequest request, StreamObserver<SummarizeAuditTrailsResponse> observer) {
        RpcSupport.unary(observer, () -> application.summarizeAuditTrails(request));
    }

    /**
     * Lists audit trails a page at a time.
     */
    @Override
    public void suggestAuditTrails(SuggestAuditTrailsRequest request, StreamObserver<SuggestAuditTrailsResponse> observer) {
        RpcSupport.unary(observer, () -> application.suggestAuditTrails(request));
    }
}
