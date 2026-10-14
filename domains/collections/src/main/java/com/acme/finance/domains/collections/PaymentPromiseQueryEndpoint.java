package com.acme.finance.domains.collections;

import com.acme.collections.v1.BrowsePaymentPromisesRequest;
import com.acme.collections.v1.BrowsePaymentPromisesResponse;
import com.acme.collections.v1.CountPaymentPromisesRequest;
import com.acme.collections.v1.CountPaymentPromisesResponse;
import com.acme.collections.v1.ExportPaymentPromisesRequest;
import com.acme.collections.v1.ExportPaymentPromisesResponse;
import com.acme.collections.v1.FindPaymentPromisesRequest;
import com.acme.collections.v1.FindPaymentPromisesResponse;
import com.acme.collections.v1.LookupPaymentPromiseRequest;
import com.acme.collections.v1.LookupPaymentPromiseResponse;
import com.acme.collections.v1.PaymentPromiseQueryServiceGrpc;
import com.acme.collections.v1.PeekPaymentPromiseRequest;
import com.acme.collections.v1.PeekPaymentPromiseResponse;
import com.acme.collections.v1.ResolvePaymentPromisesRequest;
import com.acme.collections.v1.ResolvePaymentPromisesResponse;
import com.acme.collections.v1.SuggestPaymentPromisesRequest;
import com.acme.collections.v1.SuggestPaymentPromisesResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Answers read-only questions about payment promises, for screens and reports.
 */
@GrpcService
public class PaymentPromiseQueryEndpoint extends PaymentPromiseQueryServiceGrpc.PaymentPromiseQueryServiceImplBase {
    protected final PaymentPromiseQueryApplication application;

    public PaymentPromiseQueryEndpoint(PaymentPromiseQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns a payment promise.
     */
    @Override
    public void lookupPaymentPromise(LookupPaymentPromiseRequest request, StreamObserver<LookupPaymentPromiseResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupPaymentPromise(request));
    }

    /**
     * Lists payment promises a page at a time.
     */
    @Override
    public void browsePaymentPromises(BrowsePaymentPromisesRequest request, StreamObserver<BrowsePaymentPromisesResponse> observer) {
        RpcSupport.unary(observer, () -> application.browsePaymentPromises(request));
    }

    /**
     * Lists payment promises a page at a time.
     */
    @Override
    public void findPaymentPromises(FindPaymentPromisesRequest request, StreamObserver<FindPaymentPromisesResponse> observer) {
        RpcSupport.unary(observer, () -> application.findPaymentPromises(request));
    }

    /**
     * Counts payment promises by status.
     */
    @Override
    public void countPaymentPromises(CountPaymentPromisesRequest request, StreamObserver<CountPaymentPromisesResponse> observer) {
        RpcSupport.unary(observer, () -> application.countPaymentPromises(request));
    }

    /**
     * Returns several payment promises at once.
     */
    @Override
    public void resolvePaymentPromises(ResolvePaymentPromisesRequest request, StreamObserver<ResolvePaymentPromisesResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolvePaymentPromises(request));
    }

    /**
     * Streams every matching payment promise.
     */
    @Override
    public void exportPaymentPromises(ExportPaymentPromisesRequest request, StreamObserver<ExportPaymentPromisesResponse> observer) {
        try {
            var snapshot = application.exportPaymentPromises(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Lists payment promises a page at a time.
     */
    @Override
    public void suggestPaymentPromises(SuggestPaymentPromisesRequest request, StreamObserver<SuggestPaymentPromisesResponse> observer) {
        RpcSupport.unary(observer, () -> application.suggestPaymentPromises(request));
    }

    /**
     * Returns a payment promise.
     */
    @Override
    public void peekPaymentPromise(PeekPaymentPromiseRequest request, StreamObserver<PeekPaymentPromiseResponse> observer) {
        RpcSupport.unary(observer, () -> application.peekPaymentPromise(request));
    }
}
