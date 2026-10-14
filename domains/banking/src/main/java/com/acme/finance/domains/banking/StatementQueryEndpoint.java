package com.acme.finance.domains.banking;

import com.acme.banking.v1.BrowseStatementsRequest;
import com.acme.banking.v1.BrowseStatementsResponse;
import com.acme.banking.v1.CountStatementsRequest;
import com.acme.banking.v1.CountStatementsResponse;
import com.acme.banking.v1.ExportStatementsRequest;
import com.acme.banking.v1.ExportStatementsResponse;
import com.acme.banking.v1.FindStatementsRequest;
import com.acme.banking.v1.FindStatementsResponse;
import com.acme.banking.v1.LookupStatementRequest;
import com.acme.banking.v1.LookupStatementResponse;
import com.acme.banking.v1.PeekStatementRequest;
import com.acme.banking.v1.PeekStatementResponse;
import com.acme.banking.v1.StatementQueryServiceGrpc;
import com.acme.banking.v1.SummarizeStatementsRequest;
import com.acme.banking.v1.SummarizeStatementsResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Answers read-only questions about statements, for screens and reports.
 */
@GrpcService
public class StatementQueryEndpoint extends StatementQueryServiceGrpc.StatementQueryServiceImplBase {
    protected final StatementQueryApplication application;

    public StatementQueryEndpoint(StatementQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns a statement.
     */
    @Override
    public void lookupStatement(LookupStatementRequest request, StreamObserver<LookupStatementResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupStatement(request));
    }

    /**
     * Lists statements a page at a time.
     */
    @Override
    public void browseStatements(BrowseStatementsRequest request, StreamObserver<BrowseStatementsResponse> observer) {
        RpcSupport.unary(observer, () -> application.browseStatements(request));
    }

    /**
     * Lists statements a page at a time.
     */
    @Override
    public void findStatements(FindStatementsRequest request, StreamObserver<FindStatementsResponse> observer) {
        RpcSupport.unary(observer, () -> application.findStatements(request));
    }

    /**
     * Counts statements by status.
     */
    @Override
    public void countStatements(CountStatementsRequest request, StreamObserver<CountStatementsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countStatements(request));
    }

    /**
     * Streams every matching statement.
     */
    @Override
    public void exportStatements(ExportStatementsRequest request, StreamObserver<ExportStatementsResponse> observer) {
        try {
            var snapshot = application.exportStatements(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Summarizes a statement.
     */
    @Override
    public void summarizeStatements(SummarizeStatementsRequest request, StreamObserver<SummarizeStatementsResponse> observer) {
        RpcSupport.unary(observer, () -> application.summarizeStatements(request));
    }

    /**
     * Returns a statement.
     */
    @Override
    public void peekStatement(PeekStatementRequest request, StreamObserver<PeekStatementResponse> observer) {
        RpcSupport.unary(observer, () -> application.peekStatement(request));
    }
}
