package com.acme.finance.domains.lending;

import com.acme.finance.platform.RpcSupport;
import com.acme.lending.v1.BatchGetLoanApplicationsRequest;
import com.acme.lending.v1.BatchGetLoanApplicationsResponse;
import com.acme.lending.v1.CloneLoanApplicationRequest;
import com.acme.lending.v1.CloneLoanApplicationResponse;
import com.acme.lending.v1.CreateLoanApplicationRequest;
import com.acme.lending.v1.CreateLoanApplicationResponse;
import com.acme.lending.v1.DeleteLoanApplicationRequest;
import com.acme.lending.v1.DeleteLoanApplicationResponse;
import com.acme.lending.v1.GetLoanApplicationRequest;
import com.acme.lending.v1.GetLoanApplicationResponse;
import com.acme.lending.v1.LoanApplicationServiceGrpc;
import com.acme.lending.v1.SearchLoanApplicationsRequest;
import com.acme.lending.v1.SearchLoanApplicationsResponse;
import io.grpc.stub.StreamObserver;

/**
 * Manages loan applications: creates, reads, changes, and lists them.
 */
public abstract class AbstractLoanApplicationEndpoint extends LoanApplicationServiceGrpc.LoanApplicationServiceImplBase {
    protected final LoanApplicationApplication application;

    protected AbstractLoanApplicationEndpoint(LoanApplicationApplication application) {
        this.application = application;
    }

    /**
     * Creates a loan application.
     */
    @Override
    public void createLoanApplication(CreateLoanApplicationRequest request, StreamObserver<CreateLoanApplicationResponse> observer) {
        RpcSupport.unary(observer, () -> application.createLoanApplication(request));
    }

    /**
     * Returns a loan application.
     */
    @Override
    public void getLoanApplication(GetLoanApplicationRequest request, StreamObserver<GetLoanApplicationResponse> observer) {
        RpcSupport.unary(observer, () -> application.getLoanApplication(request));
    }

    /**
     * Deletes a loan application.
     */
    @Override
    public void deleteLoanApplication(DeleteLoanApplicationRequest request, StreamObserver<DeleteLoanApplicationResponse> observer) {
        RpcSupport.unary(observer, () -> application.deleteLoanApplication(request));
    }

    /**
     * Returns several loan applications at once.
     */
    @Override
    public void batchGetLoanApplications(BatchGetLoanApplicationsRequest request, StreamObserver<BatchGetLoanApplicationsResponse> observer) {
        RpcSupport.unary(observer, () -> application.batchGetLoanApplications(request));
    }

    /**
     * Lists loan applications a page at a time.
     */
    @Override
    public void searchLoanApplications(SearchLoanApplicationsRequest request, StreamObserver<SearchLoanApplicationsResponse> observer) {
        RpcSupport.unary(observer, () -> application.searchLoanApplications(request));
    }

    /**
     * Creates a loan application.
     */
    @Override
    public void cloneLoanApplication(CloneLoanApplicationRequest request, StreamObserver<CloneLoanApplicationResponse> observer) {
        RpcSupport.unary(observer, () -> application.cloneLoanApplication(request));
    }
}
