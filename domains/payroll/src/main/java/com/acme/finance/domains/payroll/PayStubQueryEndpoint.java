package com.acme.finance.domains.payroll;

import com.acme.finance.platform.RpcSupport;
import com.acme.payroll.v1.BrowsePayStubsRequest;
import com.acme.payroll.v1.BrowsePayStubsResponse;
import com.acme.payroll.v1.CountPayStubsRequest;
import com.acme.payroll.v1.CountPayStubsResponse;
import com.acme.payroll.v1.ExportPayStubsRequest;
import com.acme.payroll.v1.ExportPayStubsResponse;
import com.acme.payroll.v1.FindPayStubsRequest;
import com.acme.payroll.v1.FindPayStubsResponse;
import com.acme.payroll.v1.LookupPayStubRequest;
import com.acme.payroll.v1.LookupPayStubResponse;
import com.acme.payroll.v1.PayStubQueryServiceGrpc;
import com.acme.payroll.v1.PeekPayStubRequest;
import com.acme.payroll.v1.PeekPayStubResponse;
import com.acme.payroll.v1.ResolvePayStubsRequest;
import com.acme.payroll.v1.ResolvePayStubsResponse;
import com.acme.payroll.v1.SummarizePayStubsRequest;
import com.acme.payroll.v1.SummarizePayStubsResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Answers read-only questions about pay stubs, for screens and reports.
 */
@GrpcService
public class PayStubQueryEndpoint extends PayStubQueryServiceGrpc.PayStubQueryServiceImplBase {
    protected final PayStubQueryApplication application;

    public PayStubQueryEndpoint(PayStubQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns a pay stub.
     */
    @Override
    public void lookupPayStub(LookupPayStubRequest request, StreamObserver<LookupPayStubResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupPayStub(request));
    }

    /**
     * Lists pay stubs a page at a time.
     */
    @Override
    public void browsePayStubs(BrowsePayStubsRequest request, StreamObserver<BrowsePayStubsResponse> observer) {
        RpcSupport.unary(observer, () -> application.browsePayStubs(request));
    }

    /**
     * Lists pay stubs a page at a time.
     */
    @Override
    public void findPayStubs(FindPayStubsRequest request, StreamObserver<FindPayStubsResponse> observer) {
        RpcSupport.unary(observer, () -> application.findPayStubs(request));
    }

    /**
     * Counts pay stubs by status.
     */
    @Override
    public void countPayStubs(CountPayStubsRequest request, StreamObserver<CountPayStubsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countPayStubs(request));
    }

    /**
     * Returns several pay stubs at once.
     */
    @Override
    public void resolvePayStubs(ResolvePayStubsRequest request, StreamObserver<ResolvePayStubsResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolvePayStubs(request));
    }

    /**
     * Streams every matching pay stub.
     */
    @Override
    public void exportPayStubs(ExportPayStubsRequest request, StreamObserver<ExportPayStubsResponse> observer) {
        try {
            var snapshot = application.exportPayStubs(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Summarizes a pay stub.
     */
    @Override
    public void summarizePayStubs(SummarizePayStubsRequest request, StreamObserver<SummarizePayStubsResponse> observer) {
        RpcSupport.unary(observer, () -> application.summarizePayStubs(request));
    }

    /**
     * Returns a pay stub.
     */
    @Override
    public void peekPayStub(PeekPayStubRequest request, StreamObserver<PeekPayStubResponse> observer) {
        RpcSupport.unary(observer, () -> application.peekPayStub(request));
    }
}
