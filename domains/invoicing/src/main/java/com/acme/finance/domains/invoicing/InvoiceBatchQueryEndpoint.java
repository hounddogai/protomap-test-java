package com.acme.finance.domains.invoicing;

import com.acme.finance.platform.RpcSupport;
import com.acme.invoicing.v1.BrowseInvoiceBatchesRequest;
import com.acme.invoicing.v1.BrowseInvoiceBatchesResponse;
import com.acme.invoicing.v1.CountInvoiceBatchesRequest;
import com.acme.invoicing.v1.CountInvoiceBatchesResponse;
import com.acme.invoicing.v1.ExportInvoiceBatchesRequest;
import com.acme.invoicing.v1.ExportInvoiceBatchesResponse;
import com.acme.invoicing.v1.FindInvoiceBatchesRequest;
import com.acme.invoicing.v1.FindInvoiceBatchesResponse;
import com.acme.invoicing.v1.InvoiceBatchQueryServiceGrpc;
import com.acme.invoicing.v1.LookupInvoiceBatchRequest;
import com.acme.invoicing.v1.LookupInvoiceBatchResponse;
import com.acme.invoicing.v1.PeekInvoiceBatchRequest;
import com.acme.invoicing.v1.PeekInvoiceBatchResponse;
import com.acme.invoicing.v1.ResolveInvoiceBatchesRequest;
import com.acme.invoicing.v1.ResolveInvoiceBatchesResponse;
import com.acme.invoicing.v1.SuggestInvoiceBatchesRequest;
import com.acme.invoicing.v1.SuggestInvoiceBatchesResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Answers read-only questions about invoice batches, for screens and reports.
 */
@GrpcService
public class InvoiceBatchQueryEndpoint extends InvoiceBatchQueryServiceGrpc.InvoiceBatchQueryServiceImplBase {
    protected final InvoiceBatchQueryApplication application;

    public InvoiceBatchQueryEndpoint(InvoiceBatchQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns an invoice batch.
     */
    @Override
    public void lookupInvoiceBatch(LookupInvoiceBatchRequest request, StreamObserver<LookupInvoiceBatchResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupInvoiceBatch(request));
    }

    /**
     * Lists invoice batches a page at a time.
     */
    @Override
    public void browseInvoiceBatches(BrowseInvoiceBatchesRequest request, StreamObserver<BrowseInvoiceBatchesResponse> observer) {
        RpcSupport.unary(observer, () -> application.browseInvoiceBatches(request));
    }

    /**
     * Lists invoice batches a page at a time.
     */
    @Override
    public void findInvoiceBatches(FindInvoiceBatchesRequest request, StreamObserver<FindInvoiceBatchesResponse> observer) {
        RpcSupport.unary(observer, () -> application.findInvoiceBatches(request));
    }

    /**
     * Counts invoice batches by status.
     */
    @Override
    public void countInvoiceBatches(CountInvoiceBatchesRequest request, StreamObserver<CountInvoiceBatchesResponse> observer) {
        RpcSupport.unary(observer, () -> application.countInvoiceBatches(request));
    }

    /**
     * Returns several invoice batches at once.
     */
    @Override
    public void resolveInvoiceBatches(ResolveInvoiceBatchesRequest request, StreamObserver<ResolveInvoiceBatchesResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveInvoiceBatches(request));
    }

    /**
     * Streams every matching invoice batch.
     */
    @Override
    public void exportInvoiceBatches(ExportInvoiceBatchesRequest request, StreamObserver<ExportInvoiceBatchesResponse> observer) {
        try {
            var snapshot = application.exportInvoiceBatches(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Lists invoice batches a page at a time.
     */
    @Override
    public void suggestInvoiceBatches(SuggestInvoiceBatchesRequest request, StreamObserver<SuggestInvoiceBatchesResponse> observer) {
        RpcSupport.unary(observer, () -> application.suggestInvoiceBatches(request));
    }

    /**
     * Returns an invoice batch.
     */
    @Override
    public void peekInvoiceBatch(PeekInvoiceBatchRequest request, StreamObserver<PeekInvoiceBatchResponse> observer) {
        RpcSupport.unary(observer, () -> application.peekInvoiceBatch(request));
    }
}
