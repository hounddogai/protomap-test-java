package com.acme.finance.domains.contracts;

import com.acme.contracts.v1.ApproveObligationRequest;
import com.acme.contracts.v1.ApproveObligationResponse;
import com.acme.contracts.v1.CommentObligationApprovalRequest;
import com.acme.contracts.v1.CommentObligationApprovalResponse;
import com.acme.contracts.v1.EscalateObligationRequest;
import com.acme.contracts.v1.EscalateObligationResponse;
import com.acme.contracts.v1.GetObligationApprovalStatusRequest;
import com.acme.contracts.v1.GetObligationApprovalStatusResponse;
import com.acme.contracts.v1.ObligationApprovalServiceGrpc;
import com.acme.contracts.v1.RecallObligationRequest;
import com.acme.contracts.v1.RecallObligationResponse;
import com.acme.contracts.v1.RejectObligationRequest;
import com.acme.contracts.v1.RejectObligationResponse;
import com.acme.contracts.v1.SubmitObligationRequest;
import com.acme.contracts.v1.SubmitObligationResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Routes obligations through approval, with escalation and delegation.
 */
public abstract class AbstractObligationApprovalEndpoint extends ObligationApprovalServiceGrpc.ObligationApprovalServiceImplBase {
    protected final ObligationApprovalApplication application;

    protected AbstractObligationApprovalEndpoint(ObligationApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits an obligation.
     */
    @Override
    public void submitObligation(SubmitObligationRequest request, StreamObserver<SubmitObligationResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitObligation(request));
    }

    /**
     * Approves an obligation.
     */
    @Override
    public void approveObligation(ApproveObligationRequest request, StreamObserver<ApproveObligationResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveObligation(request));
    }

    /**
     * Rejects an obligation.
     */
    @Override
    public void rejectObligation(RejectObligationRequest request, StreamObserver<RejectObligationResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectObligation(request));
    }

    /**
     * Escalates an obligation.
     */
    @Override
    public void escalateObligation(EscalateObligationRequest request, StreamObserver<EscalateObligationResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateObligation(request));
    }

    /**
     * Recalls an obligation.
     */
    @Override
    public void recallObligation(RecallObligationRequest request, StreamObserver<RecallObligationResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallObligation(request));
    }

    /**
     * Returns an obligation.
     */
    @Override
    public void getObligationApprovalStatus(GetObligationApprovalStatusRequest request, StreamObserver<GetObligationApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getObligationApprovalStatus(request));
    }

    /**
     * Comments an obligation.
     */
    @Override
    public void commentObligationApproval(CommentObligationApprovalRequest request, StreamObserver<CommentObligationApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentObligationApproval(request));
    }
}
