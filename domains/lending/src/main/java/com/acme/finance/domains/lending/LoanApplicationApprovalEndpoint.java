package com.acme.finance.domains.lending;

import com.acme.finance.platform.RpcSupport;
import com.acme.lending.v1.ApproveLoanApplicationRequest;
import com.acme.lending.v1.ApproveLoanApplicationResponse;
import com.acme.lending.v1.CommentLoanApplicationApprovalRequest;
import com.acme.lending.v1.CommentLoanApplicationApprovalResponse;
import com.acme.lending.v1.DelegateLoanApplicationApprovalRequest;
import com.acme.lending.v1.DelegateLoanApplicationApprovalResponse;
import com.acme.lending.v1.GetLoanApplicationApprovalStatusRequest;
import com.acme.lending.v1.GetLoanApplicationApprovalStatusResponse;
import com.acme.lending.v1.ListPendingLoanApplicationsRequest;
import com.acme.lending.v1.ListPendingLoanApplicationsResponse;
import com.acme.lending.v1.LoanApplicationApprovalServiceGrpc;
import com.acme.lending.v1.RecallLoanApplicationRequest;
import com.acme.lending.v1.RecallLoanApplicationResponse;
import com.acme.lending.v1.RejectLoanApplicationRequest;
import com.acme.lending.v1.RejectLoanApplicationResponse;
import com.acme.lending.v1.SubmitLoanApplicationRequest;
import com.acme.lending.v1.SubmitLoanApplicationResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes loan applications through approval, with escalation and delegation.
 */
@GrpcService
public class LoanApplicationApprovalEndpoint extends LoanApplicationApprovalServiceGrpc.LoanApplicationApprovalServiceImplBase {
    protected final LoanApplicationApprovalApplication application;

    public LoanApplicationApprovalEndpoint(LoanApplicationApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a loan application.
     */
    @Override
    public void submitLoanApplication(SubmitLoanApplicationRequest request, StreamObserver<SubmitLoanApplicationResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitLoanApplication(request));
    }

    /**
     * Approves a loan application.
     */
    @Override
    public void approveLoanApplication(ApproveLoanApplicationRequest request, StreamObserver<ApproveLoanApplicationResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveLoanApplication(request));
    }

    /**
     * Rejects a loan application.
     */
    @Override
    public void rejectLoanApplication(RejectLoanApplicationRequest request, StreamObserver<RejectLoanApplicationResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectLoanApplication(request));
    }

    /**
     * Lists loan applications a page at a time.
     */
    @Override
    public void listPendingLoanApplications(ListPendingLoanApplicationsRequest request, StreamObserver<ListPendingLoanApplicationsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingLoanApplications(request));
    }

    /**
     * Delegates a loan application.
     */
    @Override
    public void delegateLoanApplicationApproval(DelegateLoanApplicationApprovalRequest request, StreamObserver<DelegateLoanApplicationApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateLoanApplicationApproval(request));
    }

    /**
     * Recalls a loan application.
     */
    @Override
    public void recallLoanApplication(RecallLoanApplicationRequest request, StreamObserver<RecallLoanApplicationResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallLoanApplication(request));
    }

    /**
     * Returns a loan application.
     */
    @Override
    public void getLoanApplicationApprovalStatus(GetLoanApplicationApprovalStatusRequest request, StreamObserver<GetLoanApplicationApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getLoanApplicationApprovalStatus(request));
    }

    /**
     * Comments a loan application.
     */
    @Override
    public void commentLoanApplicationApproval(CommentLoanApplicationApprovalRequest request, StreamObserver<CommentLoanApplicationApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentLoanApplicationApproval(request));
    }
}
