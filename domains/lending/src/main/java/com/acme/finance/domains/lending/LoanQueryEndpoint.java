package com.acme.finance.domains.lending;

import com.acme.finance.platform.RpcSupport;
import com.acme.lending.v1.BrowseLoansRequest;
import com.acme.lending.v1.BrowseLoansResponse;
import com.acme.lending.v1.CountLoansRequest;
import com.acme.lending.v1.CountLoansResponse;
import com.acme.lending.v1.ExportLoansRequest;
import com.acme.lending.v1.ExportLoansResponse;
import com.acme.lending.v1.FindLoansRequest;
import com.acme.lending.v1.FindLoansResponse;
import com.acme.lending.v1.LoanQueryServiceGrpc;
import com.acme.lending.v1.LookupLoanRequest;
import com.acme.lending.v1.LookupLoanResponse;
import com.acme.lending.v1.PeekLoanRequest;
import com.acme.lending.v1.PeekLoanResponse;
import com.acme.lending.v1.ResolveLoansRequest;
import com.acme.lending.v1.ResolveLoansResponse;
import com.acme.lending.v1.SuggestLoansRequest;
import com.acme.lending.v1.SuggestLoansResponse;
import com.acme.lending.v1.SummarizeLoansRequest;
import com.acme.lending.v1.SummarizeLoansResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Answers read-only questions about loans, for screens and reports.
 */
@GrpcService
public class LoanQueryEndpoint extends LoanQueryServiceGrpc.LoanQueryServiceImplBase {
    protected final LoanQueryApplication application;

    public LoanQueryEndpoint(LoanQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns a loan.
     */
    @Override
    public void lookupLoan(LookupLoanRequest request, StreamObserver<LookupLoanResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupLoan(request));
    }

    /**
     * Lists loans a page at a time.
     */
    @Override
    public void browseLoans(BrowseLoansRequest request, StreamObserver<BrowseLoansResponse> observer) {
        RpcSupport.unary(observer, () -> application.browseLoans(request));
    }

    /**
     * Lists loans a page at a time.
     */
    @Override
    public void findLoans(FindLoansRequest request, StreamObserver<FindLoansResponse> observer) {
        RpcSupport.unary(observer, () -> application.findLoans(request));
    }

    /**
     * Counts loans by status.
     */
    @Override
    public void countLoans(CountLoansRequest request, StreamObserver<CountLoansResponse> observer) {
        RpcSupport.unary(observer, () -> application.countLoans(request));
    }

    /**
     * Returns several loans at once.
     */
    @Override
    public void resolveLoans(ResolveLoansRequest request, StreamObserver<ResolveLoansResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveLoans(request));
    }

    /**
     * Streams every matching loan.
     */
    @Override
    public void exportLoans(ExportLoansRequest request, StreamObserver<ExportLoansResponse> observer) {
        try {
            var snapshot = application.exportLoans(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Summarizes a loan.
     */
    @Override
    public void summarizeLoans(SummarizeLoansRequest request, StreamObserver<SummarizeLoansResponse> observer) {
        RpcSupport.unary(observer, () -> application.summarizeLoans(request));
    }

    /**
     * Lists loans a page at a time.
     */
    @Override
    public void suggestLoans(SuggestLoansRequest request, StreamObserver<SuggestLoansResponse> observer) {
        RpcSupport.unary(observer, () -> application.suggestLoans(request));
    }

    /**
     * Returns a loan.
     */
    @Override
    public void peekLoan(PeekLoanRequest request, StreamObserver<PeekLoanResponse> observer) {
        RpcSupport.unary(observer, () -> application.peekLoan(request));
    }
}
