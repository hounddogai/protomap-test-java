package com.acme.finance.domains.suppliers;

import com.acme.finance.platform.RpcSupport;
import com.acme.suppliers.v1.BrowseSupplierContractsRequest;
import com.acme.suppliers.v1.BrowseSupplierContractsResponse;
import com.acme.suppliers.v1.CountSupplierContractsRequest;
import com.acme.suppliers.v1.CountSupplierContractsResponse;
import com.acme.suppliers.v1.ExportSupplierContractsRequest;
import com.acme.suppliers.v1.ExportSupplierContractsResponse;
import com.acme.suppliers.v1.FindSupplierContractsRequest;
import com.acme.suppliers.v1.FindSupplierContractsResponse;
import com.acme.suppliers.v1.LookupSupplierContractRequest;
import com.acme.suppliers.v1.LookupSupplierContractResponse;
import com.acme.suppliers.v1.PeekSupplierContractRequest;
import com.acme.suppliers.v1.PeekSupplierContractResponse;
import com.acme.suppliers.v1.ResolveSupplierContractsRequest;
import com.acme.suppliers.v1.ResolveSupplierContractsResponse;
import com.acme.suppliers.v1.SuggestSupplierContractsRequest;
import com.acme.suppliers.v1.SuggestSupplierContractsResponse;
import com.acme.suppliers.v1.SummarizeSupplierContractsRequest;
import com.acme.suppliers.v1.SummarizeSupplierContractsResponse;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Answers read-only questions about supplier contracts, for screens and reports.
 */
@GrpcService
public class SupplierContractQueryEndpoint extends SupplierContractQueryServiceGrpc.SupplierContractQueryServiceImplBase {
    protected final SupplierContractQueryApplication application;

    public SupplierContractQueryEndpoint(SupplierContractQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns a supplier contract.
     */
    @Override
    public void lookupSupplierContract(LookupSupplierContractRequest request, StreamObserver<LookupSupplierContractResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupSupplierContract(request));
    }

    /**
     * Lists supplier contracts a page at a time.
     */
    @Override
    public void browseSupplierContracts(BrowseSupplierContractsRequest request, StreamObserver<BrowseSupplierContractsResponse> observer) {
        RpcSupport.unary(observer, () -> application.browseSupplierContracts(request));
    }

    /**
     * Lists supplier contracts a page at a time.
     */
    @Override
    public void findSupplierContracts(FindSupplierContractsRequest request, StreamObserver<FindSupplierContractsResponse> observer) {
        RpcSupport.unary(observer, () -> application.findSupplierContracts(request));
    }

    /**
     * Counts supplier contracts by status.
     */
    @Override
    public void countSupplierContracts(CountSupplierContractsRequest request, StreamObserver<CountSupplierContractsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countSupplierContracts(request));
    }

    /**
     * Returns several supplier contracts at once.
     */
    @Override
    public void resolveSupplierContracts(ResolveSupplierContractsRequest request, StreamObserver<ResolveSupplierContractsResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveSupplierContracts(request));
    }

    /**
     * Streams every matching supplier contract.
     */
    @Override
    public void exportSupplierContracts(ExportSupplierContractsRequest request, StreamObserver<ExportSupplierContractsResponse> observer) {
        try {
            var snapshot = application.exportSupplierContracts(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Summarizes a supplier contract.
     */
    @Override
    public void summarizeSupplierContracts(SummarizeSupplierContractsRequest request, StreamObserver<SummarizeSupplierContractsResponse> observer) {
        RpcSupport.unary(observer, () -> application.summarizeSupplierContracts(request));
    }

    /**
     * Lists supplier contracts a page at a time.
     */
    @Override
    public void suggestSupplierContracts(SuggestSupplierContractsRequest request, StreamObserver<SuggestSupplierContractsResponse> observer) {
        RpcSupport.unary(observer, () -> application.suggestSupplierContracts(request));
    }

    /**
     * Returns a supplier contract.
     */
    @Override
    public void peekSupplierContract(PeekSupplierContractRequest request, StreamObserver<PeekSupplierContractResponse> observer) {
        RpcSupport.unary(observer, () -> application.peekSupplierContract(request));
    }
}
