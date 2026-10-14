package com.acme.finance.domains.banking;

import com.acme.banking.v1.ApproveMandateRequest;
import com.acme.banking.v1.ApproveMandateResponse;
import com.acme.banking.v1.CommentMandateApprovalRequest;
import com.acme.banking.v1.CommentMandateApprovalResponse;
import com.acme.banking.v1.DelegateMandateApprovalRequest;
import com.acme.banking.v1.DelegateMandateApprovalResponse;
import com.acme.banking.v1.EscalateMandateRequest;
import com.acme.banking.v1.EscalateMandateResponse;
import com.acme.banking.v1.GetMandateApprovalStatusRequest;
import com.acme.banking.v1.GetMandateApprovalStatusResponse;
import com.acme.banking.v1.ListPendingMandatesRequest;
import com.acme.banking.v1.ListPendingMandatesResponse;
import com.acme.banking.v1.MandateApprovalServiceGrpc;
import com.acme.banking.v1.RecallMandateRequest;
import com.acme.banking.v1.RecallMandateResponse;
import com.acme.banking.v1.RejectMandateRequest;
import com.acme.banking.v1.RejectMandateResponse;
import com.acme.banking.v1.SubmitMandateRequest;
import com.acme.banking.v1.SubmitMandateResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Routes mandates through approval, with escalation and delegation.
 */
public abstract class AbstractMandateApprovalEndpoint extends MandateApprovalServiceGrpc.MandateApprovalServiceImplBase {
    protected final MandateApprovalApplication application;

    protected AbstractMandateApprovalEndpoint(MandateApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a mandate.
     */
    @Override
    public void submitMandate(SubmitMandateRequest request, StreamObserver<SubmitMandateResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitMandate(request));
    }

    /**
     * Approves a mandate.
     */
    @Override
    public void approveMandate(ApproveMandateRequest request, StreamObserver<ApproveMandateResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveMandate(request));
    }

    /**
     * Rejects a mandate.
     */
    @Override
    public void rejectMandate(RejectMandateRequest request, StreamObserver<RejectMandateResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectMandate(request));
    }

    /**
     * Escalates a mandate.
     */
    @Override
    public void escalateMandate(EscalateMandateRequest request, StreamObserver<EscalateMandateResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateMandate(request));
    }

    /**
     * Lists mandates a page at a time.
     */
    @Override
    public void listPendingMandates(ListPendingMandatesRequest request, StreamObserver<ListPendingMandatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingMandates(request));
    }

    /**
     * Delegates a mandate.
     */
    @Override
    public void delegateMandateApproval(DelegateMandateApprovalRequest request, StreamObserver<DelegateMandateApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateMandateApproval(request));
    }

    /**
     * Recalls a mandate.
     */
    @Override
    public void recallMandate(RecallMandateRequest request, StreamObserver<RecallMandateResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallMandate(request));
    }

    /**
     * Returns a mandate.
     */
    @Override
    public void getMandateApprovalStatus(GetMandateApprovalStatusRequest request, StreamObserver<GetMandateApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getMandateApprovalStatus(request));
    }

    /**
     * Comments a mandate.
     */
    @Override
    public void commentMandateApproval(CommentMandateApprovalRequest request, StreamObserver<CommentMandateApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentMandateApproval(request));
    }
}
