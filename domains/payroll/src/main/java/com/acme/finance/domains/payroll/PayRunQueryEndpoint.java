package com.acme.finance.domains.payroll;

import com.acme.finance.platform.RpcSupport;
import com.acme.payroll.v1.BrowsePayRunsRequest;
import com.acme.payroll.v1.BrowsePayRunsResponse;
import com.acme.payroll.v1.ExportPayRunsRequest;
import com.acme.payroll.v1.ExportPayRunsResponse;
import com.acme.payroll.v1.FindPayRunsRequest;
import com.acme.payroll.v1.FindPayRunsResponse;
import com.acme.payroll.v1.LookupPayRunRequest;
import com.acme.payroll.v1.LookupPayRunResponse;
import com.acme.payroll.v1.PayRunQueryServiceGrpc;
import com.acme.payroll.v1.PeekPayRunRequest;
import com.acme.payroll.v1.PeekPayRunResponse;
import com.acme.payroll.v1.ResolvePayRunsRequest;
import com.acme.payroll.v1.ResolvePayRunsResponse;
import com.acme.payroll.v1.SuggestPayRunsRequest;
import com.acme.payroll.v1.SuggestPayRunsResponse;
import com.acme.payroll.v1.SummarizePayRunsRequest;
import com.acme.payroll.v1.SummarizePayRunsResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Answers read-only questions about pay runs, for screens and reports.
 */
@GrpcService
public class PayRunQueryEndpoint extends PayRunQueryServiceGrpc.PayRunQueryServiceImplBase {
    protected final PayRunQueryApplication application;

    public PayRunQueryEndpoint(PayRunQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns a pay run.
     */
    @Override
    public void lookupPayRun(LookupPayRunRequest request, StreamObserver<LookupPayRunResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupPayRun(request));
    }

    /**
     * Lists pay runs a page at a time.
     */
    @Override
    public void browsePayRuns(BrowsePayRunsRequest request, StreamObserver<BrowsePayRunsResponse> observer) {
        RpcSupport.unary(observer, () -> application.browsePayRuns(request));
    }

    /**
     * Lists pay runs a page at a time.
     */
    @Override
    public void findPayRuns(FindPayRunsRequest request, StreamObserver<FindPayRunsResponse> observer) {
        RpcSupport.unary(observer, () -> application.findPayRuns(request));
    }

    /**
     * Returns several pay runs at once.
     */
    @Override
    public void resolvePayRuns(ResolvePayRunsRequest request, StreamObserver<ResolvePayRunsResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolvePayRuns(request));
    }

    /**
     * Streams every matching pay run.
     */
    @Override
    public void exportPayRuns(ExportPayRunsRequest request, StreamObserver<ExportPayRunsResponse> observer) {
        try {
            var snapshot = application.exportPayRuns(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Summarizes a pay run.
     */
    @Override
    public void summarizePayRuns(SummarizePayRunsRequest request, StreamObserver<SummarizePayRunsResponse> observer) {
        RpcSupport.unary(observer, () -> application.summarizePayRuns(request));
    }

    /**
     * Lists pay runs a page at a time.
     */
    @Override
    public void suggestPayRuns(SuggestPayRunsRequest request, StreamObserver<SuggestPayRunsResponse> observer) {
        RpcSupport.unary(observer, () -> application.suggestPayRuns(request));
    }

    /**
     * Returns a pay run.
     */
    @Override
    public void peekPayRun(PeekPayRunRequest request, StreamObserver<PeekPayRunResponse> observer) {
        RpcSupport.unary(observer, () -> application.peekPayRun(request));
    }
}
