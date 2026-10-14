package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.AccrualApprovalServiceGrpc;
import com.acme.accounting.v1.ApproveAccrualRequest;
import com.acme.accounting.v1.ApproveAccrualResponse;
import com.acme.accounting.v1.CommentAccrualApprovalRequest;
import com.acme.accounting.v1.CommentAccrualApprovalResponse;
import com.acme.accounting.v1.DelegateAccrualApprovalRequest;
import com.acme.accounting.v1.DelegateAccrualApprovalResponse;
import com.acme.accounting.v1.EscalateAccrualRequest;
import com.acme.accounting.v1.EscalateAccrualResponse;
import com.acme.accounting.v1.GetAccrualApprovalStatusRequest;
import com.acme.accounting.v1.GetAccrualApprovalStatusResponse;
import com.acme.accounting.v1.RecallAccrualRequest;
import com.acme.accounting.v1.RecallAccrualResponse;
import com.acme.accounting.v1.SubmitAccrualRequest;
import com.acme.accounting.v1.SubmitAccrualResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Routes accruals through approval, with escalation and delegation.
 */
public abstract class AbstractAccrualApprovalEndpoint extends AccrualApprovalServiceGrpc.AccrualApprovalServiceImplBase {
    protected final AccrualApprovalApplication application;

    protected AbstractAccrualApprovalEndpoint(AccrualApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits an accrual.
     */
    @Override
    public void submitAccrual(SubmitAccrualRequest request, StreamObserver<SubmitAccrualResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitAccrual(request));
    }

    /**
     * Approves an accrual.
     */
    @Override
    public void approveAccrual(ApproveAccrualRequest request, StreamObserver<ApproveAccrualResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveAccrual(request));
    }

    /**
     * Escalates an accrual.
     */
    @Override
    public void escalateAccrual(EscalateAccrualRequest request, StreamObserver<EscalateAccrualResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateAccrual(request));
    }

    /**
     * Delegates an accrual.
     */
    @Override
    public void delegateAccrualApproval(DelegateAccrualApprovalRequest request, StreamObserver<DelegateAccrualApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateAccrualApproval(request));
    }

    /**
     * Recalls an accrual.
     */
    @Override
    public void recallAccrual(RecallAccrualRequest request, StreamObserver<RecallAccrualResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallAccrual(request));
    }

    /**
     * Returns an accrual.
     */
    @Override
    public void getAccrualApprovalStatus(GetAccrualApprovalStatusRequest request, StreamObserver<GetAccrualApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getAccrualApprovalStatus(request));
    }

    /**
     * Comments an accrual.
     */
    @Override
    public void commentAccrualApproval(CommentAccrualApprovalRequest request, StreamObserver<CommentAccrualApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentAccrualApproval(request));
    }
}
