package com.acme.finance.domains.suppliers;

import com.acme.finance.platform.RpcSupport;
import com.acme.suppliers.v1.BrowseSuppliersRequest;
import com.acme.suppliers.v1.BrowseSuppliersResponse;
import com.acme.suppliers.v1.CountSuppliersRequest;
import com.acme.suppliers.v1.CountSuppliersResponse;
import com.acme.suppliers.v1.ExportSuppliersRequest;
import com.acme.suppliers.v1.ExportSuppliersResponse;
import com.acme.suppliers.v1.FindSuppliersRequest;
import com.acme.suppliers.v1.FindSuppliersResponse;
import com.acme.suppliers.v1.LookupSupplierRequest;
import com.acme.suppliers.v1.LookupSupplierResponse;
import com.acme.suppliers.v1.PeekSupplierRequest;
import com.acme.suppliers.v1.PeekSupplierResponse;
import com.acme.suppliers.v1.ResolveSuppliersRequest;
import com.acme.suppliers.v1.ResolveSuppliersResponse;
import com.acme.suppliers.v1.SuggestSuppliersRequest;
import com.acme.suppliers.v1.SuggestSuppliersResponse;
import com.acme.suppliers.v1.SummarizeSuppliersRequest;
import com.acme.suppliers.v1.SummarizeSuppliersResponse;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import io.grpc.stub.StreamObserver;

/**
 * Answers read-only questions about suppliers, for screens and reports.
 */
public abstract class AbstractSupplierQueryEndpoint extends SupplierQueryServiceGrpc.SupplierQueryServiceImplBase {
    protected final SupplierQueryApplication application;

    protected AbstractSupplierQueryEndpoint(SupplierQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns a supplier.
     */
    @Override
    public void lookupSupplier(LookupSupplierRequest request, StreamObserver<LookupSupplierResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupSupplier(request));
    }

    /**
     * Lists suppliers a page at a time.
     */
    @Override
    public void browseSuppliers(BrowseSuppliersRequest request, StreamObserver<BrowseSuppliersResponse> observer) {
        RpcSupport.unary(observer, () -> application.browseSuppliers(request));
    }

    /**
     * Lists suppliers a page at a time.
     */
    @Override
    public void findSuppliers(FindSuppliersRequest request, StreamObserver<FindSuppliersResponse> observer) {
        RpcSupport.unary(observer, () -> application.findSuppliers(request));
    }

    /**
     * Counts suppliers by status.
     */
    @Override
    public void countSuppliers(CountSuppliersRequest request, StreamObserver<CountSuppliersResponse> observer) {
        RpcSupport.unary(observer, () -> application.countSuppliers(request));
    }

    /**
     * Returns several suppliers at once.
     */
    @Override
    public void resolveSuppliers(ResolveSuppliersRequest request, StreamObserver<ResolveSuppliersResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveSuppliers(request));
    }

    /**
     * Streams every matching supplier.
     */
    @Override
    public void exportSuppliers(ExportSuppliersRequest request, StreamObserver<ExportSuppliersResponse> observer) {
        try {
            var snapshot = application.exportSuppliers(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Summarizes a supplier.
     */
    @Override
    public void summarizeSuppliers(SummarizeSuppliersRequest request, StreamObserver<SummarizeSuppliersResponse> observer) {
        RpcSupport.unary(observer, () -> application.summarizeSuppliers(request));
    }

    /**
     * Lists suppliers a page at a time.
     */
    @Override
    public void suggestSuppliers(SuggestSuppliersRequest request, StreamObserver<SuggestSuppliersResponse> observer) {
        RpcSupport.unary(observer, () -> application.suggestSuppliers(request));
    }

    /**
     * Returns a supplier.
     */
    @Override
    public void peekSupplier(PeekSupplierRequest request, StreamObserver<PeekSupplierResponse> observer) {
        RpcSupport.unary(observer, () -> application.peekSupplier(request));
    }
}
