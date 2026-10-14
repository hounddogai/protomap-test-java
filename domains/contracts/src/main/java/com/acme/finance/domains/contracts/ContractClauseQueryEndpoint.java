package com.acme.finance.domains.contracts;

import com.acme.contracts.v1.BrowseContractClausesRequest;
import com.acme.contracts.v1.BrowseContractClausesResponse;
import com.acme.contracts.v1.ContractClauseQueryServiceGrpc;
import com.acme.contracts.v1.CountContractClausesRequest;
import com.acme.contracts.v1.CountContractClausesResponse;
import com.acme.contracts.v1.ExportContractClausesRequest;
import com.acme.contracts.v1.ExportContractClausesResponse;
import com.acme.contracts.v1.FindContractClausesRequest;
import com.acme.contracts.v1.FindContractClausesResponse;
import com.acme.contracts.v1.LookupContractClauseRequest;
import com.acme.contracts.v1.LookupContractClauseResponse;
import com.acme.contracts.v1.PeekContractClauseRequest;
import com.acme.contracts.v1.PeekContractClauseResponse;
import com.acme.contracts.v1.ResolveContractClausesRequest;
import com.acme.contracts.v1.ResolveContractClausesResponse;
import com.acme.contracts.v1.SummarizeContractClausesRequest;
import com.acme.contracts.v1.SummarizeContractClausesResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Answers read-only questions about contract clauses, for screens and reports.
 */
@GrpcService
public class ContractClauseQueryEndpoint extends ContractClauseQueryServiceGrpc.ContractClauseQueryServiceImplBase {
    protected final ContractClauseQueryApplication application;

    public ContractClauseQueryEndpoint(ContractClauseQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns a contract clause.
     */
    @Override
    public void lookupContractClause(LookupContractClauseRequest request, StreamObserver<LookupContractClauseResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupContractClause(request));
    }

    /**
     * Lists contract clauses a page at a time.
     */
    @Override
    public void browseContractClauses(BrowseContractClausesRequest request, StreamObserver<BrowseContractClausesResponse> observer) {
        RpcSupport.unary(observer, () -> application.browseContractClauses(request));
    }

    /**
     * Lists contract clauses a page at a time.
     */
    @Override
    public void findContractClauses(FindContractClausesRequest request, StreamObserver<FindContractClausesResponse> observer) {
        RpcSupport.unary(observer, () -> application.findContractClauses(request));
    }

    /**
     * Counts contract clauses by status.
     */
    @Override
    public void countContractClauses(CountContractClausesRequest request, StreamObserver<CountContractClausesResponse> observer) {
        RpcSupport.unary(observer, () -> application.countContractClauses(request));
    }

    /**
     * Returns several contract clauses at once.
     */
    @Override
    public void resolveContractClauses(ResolveContractClausesRequest request, StreamObserver<ResolveContractClausesResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveContractClauses(request));
    }

    /**
     * Streams every matching contract clause.
     */
    @Override
    public void exportContractClauses(ExportContractClausesRequest request, StreamObserver<ExportContractClausesResponse> observer) {
        try {
            var snapshot = application.exportContractClauses(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Summarizes a contract clause.
     */
    @Override
    public void summarizeContractClauses(SummarizeContractClausesRequest request, StreamObserver<SummarizeContractClausesResponse> observer) {
        RpcSupport.unary(observer, () -> application.summarizeContractClauses(request));
    }

    /**
     * Returns a contract clause.
     */
    @Override
    public void peekContractClause(PeekContractClauseRequest request, StreamObserver<PeekContractClauseResponse> observer) {
        RpcSupport.unary(observer, () -> application.peekContractClause(request));
    }
}
