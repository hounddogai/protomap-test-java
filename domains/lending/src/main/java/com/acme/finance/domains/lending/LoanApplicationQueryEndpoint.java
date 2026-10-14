package com.acme.finance.domains.lending;

import com.acme.finance.platform.RpcSupport;
import com.acme.lending.v1.BrowseLoanApplicationsRequest;
import com.acme.lending.v1.BrowseLoanApplicationsResponse;
import com.acme.lending.v1.CountLoanApplicationsRequest;
import com.acme.lending.v1.CountLoanApplicationsResponse;
import com.acme.lending.v1.ExportLoanApplicationsRequest;
import com.acme.lending.v1.ExportLoanApplicationsResponse;
import com.acme.lending.v1.LoanApplicationQueryServiceGrpc;
import com.acme.lending.v1.LookupLoanApplicationRequest;
import com.acme.lending.v1.LookupLoanApplicationResponse;
import com.acme.lending.v1.ResolveLoanApplicationsRequest;
import com.acme.lending.v1.ResolveLoanApplicationsResponse;
import com.acme.lending.v1.SuggestLoanApplicationsRequest;
import com.acme.lending.v1.SuggestLoanApplicationsResponse;
import com.acme.lending.v1.SummarizeLoanApplicationsRequest;
import com.acme.lending.v1.SummarizeLoanApplicationsResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Answers read-only questions about loan applications, for screens and reports.
 */
@GrpcService
public class LoanApplicationQueryEndpoint extends LoanApplicationQueryServiceGrpc.LoanApplicationQueryServiceImplBase {
    protected final LoanApplicationQueryApplication application;

    public LoanApplicationQueryEndpoint(LoanApplicationQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns a loan application.
     */
    @Override
    public void lookupLoanApplication(LookupLoanApplicationRequest request, StreamObserver<LookupLoanApplicationResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupLoanApplication(request));
    }

    /**
     * Lists loan applications a page at a time.
     */
    @Override
    public void browseLoanApplications(BrowseLoanApplicationsRequest request, StreamObserver<BrowseLoanApplicationsResponse> observer) {
        RpcSupport.unary(observer, () -> application.browseLoanApplications(request));
    }

    /**
     * Counts loan applications by status.
     */
    @Override
    public void countLoanApplications(CountLoanApplicationsRequest request, StreamObserver<CountLoanApplicationsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countLoanApplications(request));
    }

    /**
     * Returns several loan applications at once.
     */
    @Override
    public void resolveLoanApplications(ResolveLoanApplicationsRequest request, StreamObserver<ResolveLoanApplicationsResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveLoanApplications(request));
    }

    /**
     * Streams every matching loan application.
     */
    @Override
    public void exportLoanApplications(ExportLoanApplicationsRequest request, StreamObserver<ExportLoanApplicationsResponse> observer) {
        try {
            var snapshot = application.exportLoanApplications(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Summarizes a loan application.
     */
    @Override
    public void summarizeLoanApplications(SummarizeLoanApplicationsRequest request, StreamObserver<SummarizeLoanApplicationsResponse> observer) {
        RpcSupport.unary(observer, () -> application.summarizeLoanApplications(request));
    }

    /**
     * Lists loan applications a page at a time.
     */
    @Override
    public void suggestLoanApplications(SuggestLoanApplicationsRequest request, StreamObserver<SuggestLoanApplicationsResponse> observer) {
        RpcSupport.unary(observer, () -> application.suggestLoanApplications(request));
    }
}
