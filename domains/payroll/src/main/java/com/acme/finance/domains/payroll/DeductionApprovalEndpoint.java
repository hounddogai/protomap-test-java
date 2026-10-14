package com.acme.finance.domains.payroll;

import com.acme.finance.platform.RpcSupport;
import com.acme.payroll.v1.ApproveDeductionRequest;
import com.acme.payroll.v1.ApproveDeductionResponse;
import com.acme.payroll.v1.CommentDeductionApprovalRequest;
import com.acme.payroll.v1.CommentDeductionApprovalResponse;
import com.acme.payroll.v1.DeductionApprovalServiceGrpc;
import com.acme.payroll.v1.DelegateDeductionApprovalRequest;
import com.acme.payroll.v1.DelegateDeductionApprovalResponse;
import com.acme.payroll.v1.EscalateDeductionRequest;
import com.acme.payroll.v1.EscalateDeductionResponse;
import com.acme.payroll.v1.GetDeductionApprovalStatusRequest;
import com.acme.payroll.v1.GetDeductionApprovalStatusResponse;
import com.acme.payroll.v1.ListPendingDeductionsRequest;
import com.acme.payroll.v1.ListPendingDeductionsResponse;
import com.acme.payroll.v1.RecallDeductionRequest;
import com.acme.payroll.v1.RecallDeductionResponse;
import com.acme.payroll.v1.RejectDeductionRequest;
import com.acme.payroll.v1.RejectDeductionResponse;
import com.acme.payroll.v1.SubmitDeductionRequest;
import com.acme.payroll.v1.SubmitDeductionResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes deductions through approval, with escalation and delegation.
 */
@GrpcService
public class DeductionApprovalEndpoint extends DeductionApprovalServiceGrpc.DeductionApprovalServiceImplBase {
    protected final DeductionApprovalApplication application;

    public DeductionApprovalEndpoint(DeductionApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a deduction.
     */
    @Override
    public void submitDeduction(SubmitDeductionRequest request, StreamObserver<SubmitDeductionResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitDeduction(request));
    }

    /**
     * Approves a deduction.
     */
    @Override
    public void approveDeduction(ApproveDeductionRequest request, StreamObserver<ApproveDeductionResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveDeduction(request));
    }

    /**
     * Rejects a deduction.
     */
    @Override
    public void rejectDeduction(RejectDeductionRequest request, StreamObserver<RejectDeductionResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectDeduction(request));
    }

    /**
     * Escalates a deduction.
     */
    @Override
    public void escalateDeduction(EscalateDeductionRequest request, StreamObserver<EscalateDeductionResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateDeduction(request));
    }

    /**
     * Lists deductions a page at a time.
     */
    @Override
    public void listPendingDeductions(ListPendingDeductionsRequest request, StreamObserver<ListPendingDeductionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingDeductions(request));
    }

    /**
     * Delegates a deduction.
     */
    @Override
    public void delegateDeductionApproval(DelegateDeductionApprovalRequest request, StreamObserver<DelegateDeductionApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateDeductionApproval(request));
    }

    /**
     * Recalls a deduction.
     */
    @Override
    public void recallDeduction(RecallDeductionRequest request, StreamObserver<RecallDeductionResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallDeduction(request));
    }

    /**
     * Returns a deduction.
     */
    @Override
    public void getDeductionApprovalStatus(GetDeductionApprovalStatusRequest request, StreamObserver<GetDeductionApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getDeductionApprovalStatus(request));
    }

    /**
     * Comments a deduction.
     */
    @Override
    public void commentDeductionApproval(CommentDeductionApprovalRequest request, StreamObserver<CommentDeductionApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentDeductionApproval(request));
    }
}
