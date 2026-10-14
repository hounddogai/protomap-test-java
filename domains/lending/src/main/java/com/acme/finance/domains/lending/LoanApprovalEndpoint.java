package com.acme.finance.domains.lending;

import com.acme.finance.platform.RpcSupport;
import com.acme.lending.v1.ApproveLoanRequest;
import com.acme.lending.v1.ApproveLoanResponse;
import com.acme.lending.v1.CommentLoanApprovalRequest;
import com.acme.lending.v1.CommentLoanApprovalResponse;
import com.acme.lending.v1.EscalateLoanRequest;
import com.acme.lending.v1.EscalateLoanResponse;
import com.acme.lending.v1.GetLoanApprovalStatusRequest;
import com.acme.lending.v1.GetLoanApprovalStatusResponse;
import com.acme.lending.v1.ListPendingLoansRequest;
import com.acme.lending.v1.ListPendingLoansResponse;
import com.acme.lending.v1.LoanApprovalServiceGrpc;
import com.acme.lending.v1.RecallLoanRequest;
import com.acme.lending.v1.RecallLoanResponse;
import com.acme.lending.v1.RejectLoanRequest;
import com.acme.lending.v1.RejectLoanResponse;
import com.acme.lending.v1.SubmitLoanRequest;
import com.acme.lending.v1.SubmitLoanResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes loans through approval, with escalation and delegation.
 */
@GrpcService
public class LoanApprovalEndpoint extends LoanApprovalServiceGrpc.LoanApprovalServiceImplBase {
    protected final LoanApprovalApplication application;

    public LoanApprovalEndpoint(LoanApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a loan.
     */
    @Override
    public void submitLoan(SubmitLoanRequest request, StreamObserver<SubmitLoanResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitLoan(request));
    }

    /**
     * Approves a loan.
     */
    @Override
    public void approveLoan(ApproveLoanRequest request, StreamObserver<ApproveLoanResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveLoan(request));
    }

    /**
     * Rejects a loan.
     */
    @Override
    public void rejectLoan(RejectLoanRequest request, StreamObserver<RejectLoanResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectLoan(request));
    }

    /**
     * Escalates a loan.
     */
    @Override
    public void escalateLoan(EscalateLoanRequest request, StreamObserver<EscalateLoanResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateLoan(request));
    }

    /**
     * Lists loans a page at a time.
     */
    @Override
    public void listPendingLoans(ListPendingLoansRequest request, StreamObserver<ListPendingLoansResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingLoans(request));
    }

    /**
     * Recalls a loan.
     */
    @Override
    public void recallLoan(RecallLoanRequest request, StreamObserver<RecallLoanResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallLoan(request));
    }

    /**
     * Returns a loan.
     */
    @Override
    public void getLoanApprovalStatus(GetLoanApprovalStatusRequest request, StreamObserver<GetLoanApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getLoanApprovalStatus(request));
    }

    /**
     * Comments a loan.
     */
    @Override
    public void commentLoanApproval(CommentLoanApprovalRequest request, StreamObserver<CommentLoanApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentLoanApproval(request));
    }
}
