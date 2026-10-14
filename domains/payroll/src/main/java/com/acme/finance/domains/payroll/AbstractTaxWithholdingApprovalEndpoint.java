package com.acme.finance.domains.payroll;

import com.acme.finance.platform.RpcSupport;
import com.acme.payroll.v1.ApproveTaxWithholdingRequest;
import com.acme.payroll.v1.ApproveTaxWithholdingResponse;
import com.acme.payroll.v1.CommentTaxWithholdingApprovalRequest;
import com.acme.payroll.v1.CommentTaxWithholdingApprovalResponse;
import com.acme.payroll.v1.DelegateTaxWithholdingApprovalRequest;
import com.acme.payroll.v1.DelegateTaxWithholdingApprovalResponse;
import com.acme.payroll.v1.EscalateTaxWithholdingRequest;
import com.acme.payroll.v1.EscalateTaxWithholdingResponse;
import com.acme.payroll.v1.GetTaxWithholdingApprovalStatusRequest;
import com.acme.payroll.v1.GetTaxWithholdingApprovalStatusResponse;
import com.acme.payroll.v1.RecallTaxWithholdingRequest;
import com.acme.payroll.v1.RecallTaxWithholdingResponse;
import com.acme.payroll.v1.RejectTaxWithholdingRequest;
import com.acme.payroll.v1.RejectTaxWithholdingResponse;
import com.acme.payroll.v1.SubmitTaxWithholdingRequest;
import com.acme.payroll.v1.SubmitTaxWithholdingResponse;
import com.acme.payroll.v1.TaxWithholdingApprovalServiceGrpc;
import io.grpc.stub.StreamObserver;

/**
 * Routes tax withholdings through approval, with escalation and delegation.
 */
public abstract class AbstractTaxWithholdingApprovalEndpoint extends TaxWithholdingApprovalServiceGrpc.TaxWithholdingApprovalServiceImplBase {
    protected final TaxWithholdingApprovalApplication application;

    protected AbstractTaxWithholdingApprovalEndpoint(TaxWithholdingApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a tax withholding.
     */
    @Override
    public void submitTaxWithholding(SubmitTaxWithholdingRequest request, StreamObserver<SubmitTaxWithholdingResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitTaxWithholding(request));
    }

    /**
     * Approves a tax withholding.
     */
    @Override
    public void approveTaxWithholding(ApproveTaxWithholdingRequest request, StreamObserver<ApproveTaxWithholdingResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveTaxWithholding(request));
    }

    /**
     * Rejects a tax withholding.
     */
    @Override
    public void rejectTaxWithholding(RejectTaxWithholdingRequest request, StreamObserver<RejectTaxWithholdingResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectTaxWithholding(request));
    }

    /**
     * Escalates a tax withholding.
     */
    @Override
    public void escalateTaxWithholding(EscalateTaxWithholdingRequest request, StreamObserver<EscalateTaxWithholdingResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateTaxWithholding(request));
    }

    /**
     * Delegates a tax withholding.
     */
    @Override
    public void delegateTaxWithholdingApproval(DelegateTaxWithholdingApprovalRequest request, StreamObserver<DelegateTaxWithholdingApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateTaxWithholdingApproval(request));
    }

    /**
     * Recalls a tax withholding.
     */
    @Override
    public void recallTaxWithholding(RecallTaxWithholdingRequest request, StreamObserver<RecallTaxWithholdingResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallTaxWithholding(request));
    }

    /**
     * Returns a tax withholding.
     */
    @Override
    public void getTaxWithholdingApprovalStatus(GetTaxWithholdingApprovalStatusRequest request, StreamObserver<GetTaxWithholdingApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getTaxWithholdingApprovalStatus(request));
    }

    /**
     * Comments a tax withholding.
     */
    @Override
    public void commentTaxWithholdingApproval(CommentTaxWithholdingApprovalRequest request, StreamObserver<CommentTaxWithholdingApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentTaxWithholdingApproval(request));
    }
}
