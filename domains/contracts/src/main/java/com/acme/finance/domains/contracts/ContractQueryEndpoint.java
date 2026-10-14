package com.acme.finance.domains.contracts;

import com.acme.contracts.v1.BrowseContractsRequest;
import com.acme.contracts.v1.BrowseContractsResponse;
import com.acme.contracts.v1.ContractQueryServiceGrpc;
import com.acme.contracts.v1.CountContractsRequest;
import com.acme.contracts.v1.CountContractsResponse;
import com.acme.contracts.v1.ExportContractsRequest;
import com.acme.contracts.v1.ExportContractsResponse;
import com.acme.contracts.v1.FindContractsRequest;
import com.acme.contracts.v1.FindContractsResponse;
import com.acme.contracts.v1.LookupContractRequest;
import com.acme.contracts.v1.LookupContractResponse;
import com.acme.contracts.v1.PeekContractRequest;
import com.acme.contracts.v1.PeekContractResponse;
import com.acme.contracts.v1.ResolveContractsRequest;
import com.acme.contracts.v1.ResolveContractsResponse;
import com.acme.contracts.v1.SuggestContractsRequest;
import com.acme.contracts.v1.SuggestContractsResponse;
import com.acme.contracts.v1.SummarizeContractsRequest;
import com.acme.contracts.v1.SummarizeContractsResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Answers read-only questions about contracts, for screens and reports.
 */
@GrpcService
public class ContractQueryEndpoint extends ContractQueryServiceGrpc.ContractQueryServiceImplBase {
    protected final ContractQueryApplication application;

    public ContractQueryEndpoint(ContractQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns a contract.
     */
    @Override
    public void lookupContract(LookupContractRequest request, StreamObserver<LookupContractResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupContract(request));
    }

    /**
     * Lists contracts a page at a time.
     */
    @Override
    public void browseContracts(BrowseContractsRequest request, StreamObserver<BrowseContractsResponse> observer) {
        RpcSupport.unary(observer, () -> application.browseContracts(request));
    }

    /**
     * Lists contracts a page at a time.
     */
    @Override
    public void findContracts(FindContractsRequest request, StreamObserver<FindContractsResponse> observer) {
        RpcSupport.unary(observer, () -> application.findContracts(request));
    }

    /**
     * Counts contracts by status.
     */
    @Override
    public void countContracts(CountContractsRequest request, StreamObserver<CountContractsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countContracts(request));
    }

    /**
     * Returns several contracts at once.
     */
    @Override
    public void resolveContracts(ResolveContractsRequest request, StreamObserver<ResolveContractsResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveContracts(request));
    }

    /**
     * Streams every matching contract.
     */
    @Override
    public void exportContracts(ExportContractsRequest request, StreamObserver<ExportContractsResponse> observer) {
        try {
            var snapshot = application.exportContracts(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Summarizes a contract.
     */
    @Override
    public void summarizeContracts(SummarizeContractsRequest request, StreamObserver<SummarizeContractsResponse> observer) {
        RpcSupport.unary(observer, () -> application.summarizeContracts(request));
    }

    /**
     * Lists contracts a page at a time.
     */
    @Override
    public void suggestContracts(SuggestContractsRequest request, StreamObserver<SuggestContractsResponse> observer) {
        RpcSupport.unary(observer, () -> application.suggestContracts(request));
    }

    /**
     * Returns a contract.
     */
    @Override
    public void peekContract(PeekContractRequest request, StreamObserver<PeekContractResponse> observer) {
        RpcSupport.unary(observer, () -> application.peekContract(request));
    }
}
