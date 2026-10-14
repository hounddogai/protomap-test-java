package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.ApproveReconciliationRequest;
import com.acme.accounting.v1.ApproveReconciliationResponse;
import com.acme.accounting.v1.CommentReconciliationApprovalRequest;
import com.acme.accounting.v1.CommentReconciliationApprovalResponse;
import com.acme.accounting.v1.DelegateReconciliationApprovalRequest;
import com.acme.accounting.v1.DelegateReconciliationApprovalResponse;
import com.acme.accounting.v1.EscalateReconciliationRequest;
import com.acme.accounting.v1.EscalateReconciliationResponse;
import com.acme.accounting.v1.GetReconciliationApprovalStatusRequest;
import com.acme.accounting.v1.GetReconciliationApprovalStatusResponse;
import com.acme.accounting.v1.RecallReconciliationRequest;
import com.acme.accounting.v1.RecallReconciliationResponse;
import com.acme.accounting.v1.ReconciliationApprovalServiceGrpc;
import com.acme.accounting.v1.RejectReconciliationRequest;
import com.acme.accounting.v1.RejectReconciliationResponse;
import com.acme.accounting.v1.SubmitReconciliationRequest;
import com.acme.accounting.v1.SubmitReconciliationResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes reconciliations through approval, with escalation and delegation.
 */
@GrpcService
public class ReconciliationApprovalEndpoint extends ReconciliationApprovalServiceGrpc.ReconciliationApprovalServiceImplBase {
    protected final ReconciliationApprovalApplication application;

    public ReconciliationApprovalEndpoint(ReconciliationApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a reconciliation.
     */
    @Override
    public void submitReconciliation(SubmitReconciliationRequest request, StreamObserver<SubmitReconciliationResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitReconciliation(request));
    }

    /**
     * Approves a reconciliation.
     */
    @Override
    public void approveReconciliation(ApproveReconciliationRequest request, StreamObserver<ApproveReconciliationResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveReconciliation(request));
    }

    /**
     * Rejects a reconciliation.
     */
    @Override
    public void rejectReconciliation(RejectReconciliationRequest request, StreamObserver<RejectReconciliationResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectReconciliation(request));
    }

    /**
     * Escalates a reconciliation.
     */
    @Override
    public void escalateReconciliation(EscalateReconciliationRequest request, StreamObserver<EscalateReconciliationResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateReconciliation(request));
    }

    /**
     * Delegates a reconciliation.
     */
    @Override
    public void delegateReconciliationApproval(DelegateReconciliationApprovalRequest request, StreamObserver<DelegateReconciliationApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateReconciliationApproval(request));
    }

    /**
     * Recalls a reconciliation.
     */
    @Override
    public void recallReconciliation(RecallReconciliationRequest request, StreamObserver<RecallReconciliationResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallReconciliation(request));
    }

    /**
     * Returns a reconciliation.
     */
    @Override
    public void getReconciliationApprovalStatus(GetReconciliationApprovalStatusRequest request, StreamObserver<GetReconciliationApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getReconciliationApprovalStatus(request));
    }

    /**
     * Comments a reconciliation.
     */
    @Override
    public void commentReconciliationApproval(CommentReconciliationApprovalRequest request, StreamObserver<CommentReconciliationApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentReconciliationApproval(request));
    }
}
