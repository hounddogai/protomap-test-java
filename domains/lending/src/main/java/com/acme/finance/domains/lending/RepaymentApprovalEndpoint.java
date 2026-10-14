package com.acme.finance.domains.lending;

import com.acme.finance.platform.RpcSupport;
import com.acme.lending.v1.ApproveRepaymentRequest;
import com.acme.lending.v1.ApproveRepaymentResponse;
import com.acme.lending.v1.CommentRepaymentApprovalRequest;
import com.acme.lending.v1.CommentRepaymentApprovalResponse;
import com.acme.lending.v1.EscalateRepaymentRequest;
import com.acme.lending.v1.EscalateRepaymentResponse;
import com.acme.lending.v1.GetRepaymentApprovalStatusRequest;
import com.acme.lending.v1.GetRepaymentApprovalStatusResponse;
import com.acme.lending.v1.ListPendingRepaymentsRequest;
import com.acme.lending.v1.ListPendingRepaymentsResponse;
import com.acme.lending.v1.RecallRepaymentRequest;
import com.acme.lending.v1.RecallRepaymentResponse;
import com.acme.lending.v1.RejectRepaymentRequest;
import com.acme.lending.v1.RejectRepaymentResponse;
import com.acme.lending.v1.RepaymentApprovalServiceGrpc;
import com.acme.lending.v1.SubmitRepaymentRequest;
import com.acme.lending.v1.SubmitRepaymentResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes repayments through approval, with escalation and delegation.
 */
@GrpcService
public class RepaymentApprovalEndpoint extends RepaymentApprovalServiceGrpc.RepaymentApprovalServiceImplBase {
    protected final RepaymentApprovalApplication application;

    public RepaymentApprovalEndpoint(RepaymentApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a repayment.
     */
    @Override
    public void submitRepayment(SubmitRepaymentRequest request, StreamObserver<SubmitRepaymentResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitRepayment(request));
    }

    /**
     * Approves a repayment.
     */
    @Override
    public void approveRepayment(ApproveRepaymentRequest request, StreamObserver<ApproveRepaymentResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveRepayment(request));
    }

    /**
     * Rejects a repayment.
     */
    @Override
    public void rejectRepayment(RejectRepaymentRequest request, StreamObserver<RejectRepaymentResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectRepayment(request));
    }

    /**
     * Escalates a repayment.
     */
    @Override
    public void escalateRepayment(EscalateRepaymentRequest request, StreamObserver<EscalateRepaymentResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateRepayment(request));
    }

    /**
     * Lists repayments a page at a time.
     */
    @Override
    public void listPendingRepayments(ListPendingRepaymentsRequest request, StreamObserver<ListPendingRepaymentsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingRepayments(request));
    }

    /**
     * Recalls a repayment.
     */
    @Override
    public void recallRepayment(RecallRepaymentRequest request, StreamObserver<RecallRepaymentResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallRepayment(request));
    }

    /**
     * Returns a repayment.
     */
    @Override
    public void getRepaymentApprovalStatus(GetRepaymentApprovalStatusRequest request, StreamObserver<GetRepaymentApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getRepaymentApprovalStatus(request));
    }

    /**
     * Comments a repayment.
     */
    @Override
    public void commentRepaymentApproval(CommentRepaymentApprovalRequest request, StreamObserver<CommentRepaymentApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentRepaymentApproval(request));
    }
}
