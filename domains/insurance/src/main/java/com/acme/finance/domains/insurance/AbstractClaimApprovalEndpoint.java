package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.ApproveClaimRequest;
import com.acme.insurance.v1.ApproveClaimResponse;
import com.acme.insurance.v1.ClaimApprovalServiceGrpc;
import com.acme.insurance.v1.CommentClaimApprovalRequest;
import com.acme.insurance.v1.CommentClaimApprovalResponse;
import com.acme.insurance.v1.DelegateClaimApprovalRequest;
import com.acme.insurance.v1.DelegateClaimApprovalResponse;
import com.acme.insurance.v1.EscalateClaimRequest;
import com.acme.insurance.v1.EscalateClaimResponse;
import com.acme.insurance.v1.ListPendingClaimsRequest;
import com.acme.insurance.v1.ListPendingClaimsResponse;
import com.acme.insurance.v1.RecallClaimRequest;
import com.acme.insurance.v1.RecallClaimResponse;
import com.acme.insurance.v1.RejectClaimRequest;
import com.acme.insurance.v1.RejectClaimResponse;
import com.acme.insurance.v1.SubmitClaimRequest;
import com.acme.insurance.v1.SubmitClaimResponse;
import io.grpc.stub.StreamObserver;

/**
 * Routes claims through approval, with escalation and delegation.
 */
public abstract class AbstractClaimApprovalEndpoint extends ClaimApprovalServiceGrpc.ClaimApprovalServiceImplBase {
    protected final ClaimApprovalApplication application;

    protected AbstractClaimApprovalEndpoint(ClaimApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a claim.
     */
    @Override
    public void submitClaim(SubmitClaimRequest request, StreamObserver<SubmitClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitClaim(request));
    }

    /**
     * Approves a claim.
     */
    @Override
    public void approveClaim(ApproveClaimRequest request, StreamObserver<ApproveClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveClaim(request));
    }

    /**
     * Rejects a claim.
     */
    @Override
    public void rejectClaim(RejectClaimRequest request, StreamObserver<RejectClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectClaim(request));
    }

    /**
     * Escalates a claim.
     */
    @Override
    public void escalateClaim(EscalateClaimRequest request, StreamObserver<EscalateClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateClaim(request));
    }

    /**
     * Lists claims a page at a time.
     */
    @Override
    public void listPendingClaims(ListPendingClaimsRequest request, StreamObserver<ListPendingClaimsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingClaims(request));
    }

    /**
     * Delegates a claim.
     */
    @Override
    public void delegateClaimApproval(DelegateClaimApprovalRequest request, StreamObserver<DelegateClaimApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateClaimApproval(request));
    }

    /**
     * Recalls a claim.
     */
    @Override
    public void recallClaim(RecallClaimRequest request, StreamObserver<RecallClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallClaim(request));
    }

    /**
     * Comments a claim.
     */
    @Override
    public void commentClaimApproval(CommentClaimApprovalRequest request, StreamObserver<CommentClaimApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentClaimApproval(request));
    }
}
