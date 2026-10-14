package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.AccrualQueryServiceGrpc;
import com.acme.accounting.v1.BrowseAccrualsRequest;
import com.acme.accounting.v1.BrowseAccrualsResponse;
import com.acme.accounting.v1.CountAccrualsRequest;
import com.acme.accounting.v1.CountAccrualsResponse;
import com.acme.accounting.v1.ExportAccrualsRequest;
import com.acme.accounting.v1.ExportAccrualsResponse;
import com.acme.accounting.v1.FindAccrualsRequest;
import com.acme.accounting.v1.FindAccrualsResponse;
import com.acme.accounting.v1.LookupAccrualRequest;
import com.acme.accounting.v1.LookupAccrualResponse;
import com.acme.accounting.v1.PeekAccrualRequest;
import com.acme.accounting.v1.PeekAccrualResponse;
import com.acme.accounting.v1.ResolveAccrualsRequest;
import com.acme.accounting.v1.ResolveAccrualsResponse;
import com.acme.accounting.v1.SuggestAccrualsRequest;
import com.acme.accounting.v1.SuggestAccrualsResponse;
import com.acme.accounting.v1.SummarizeAccrualsRequest;
import com.acme.accounting.v1.SummarizeAccrualsResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Answers read-only questions about accruals, for screens and reports.
 */
@GrpcService
public class AccrualQueryEndpoint extends AccrualQueryServiceGrpc.AccrualQueryServiceImplBase {
    protected final AccrualQueryApplication application;

    public AccrualQueryEndpoint(AccrualQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns an accrual.
     */
    @Override
    public void lookupAccrual(LookupAccrualRequest request, StreamObserver<LookupAccrualResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupAccrual(request));
    }

    /**
     * Lists accruals a page at a time.
     */
    @Override
    public void browseAccruals(BrowseAccrualsRequest request, StreamObserver<BrowseAccrualsResponse> observer) {
        RpcSupport.unary(observer, () -> application.browseAccruals(request));
    }

    /**
     * Lists accruals a page at a time.
     */
    @Override
    public void findAccruals(FindAccrualsRequest request, StreamObserver<FindAccrualsResponse> observer) {
        RpcSupport.unary(observer, () -> application.findAccruals(request));
    }

    /**
     * Counts accruals by status.
     */
    @Override
    public void countAccruals(CountAccrualsRequest request, StreamObserver<CountAccrualsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countAccruals(request));
    }

    /**
     * Returns several accruals at once.
     */
    @Override
    public void resolveAccruals(ResolveAccrualsRequest request, StreamObserver<ResolveAccrualsResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveAccruals(request));
    }

    /**
     * Streams every matching accrual.
     */
    @Override
    public void exportAccruals(ExportAccrualsRequest request, StreamObserver<ExportAccrualsResponse> observer) {
        try {
            var snapshot = application.exportAccruals(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Summarizes an accrual.
     */
    @Override
    public void summarizeAccruals(SummarizeAccrualsRequest request, StreamObserver<SummarizeAccrualsResponse> observer) {
        RpcSupport.unary(observer, () -> application.summarizeAccruals(request));
    }

    /**
     * Lists accruals a page at a time.
     */
    @Override
    public void suggestAccruals(SuggestAccrualsRequest request, StreamObserver<SuggestAccrualsResponse> observer) {
        RpcSupport.unary(observer, () -> application.suggestAccruals(request));
    }

    /**
     * Returns an accrual.
     */
    @Override
    public void peekAccrual(PeekAccrualRequest request, StreamObserver<PeekAccrualResponse> observer) {
        RpcSupport.unary(observer, () -> application.peekAccrual(request));
    }
}
