package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.ApproveVarianceRequest;
import com.acme.budgeting.v1.ApproveVarianceResponse;
import com.acme.budgeting.v1.CommentVarianceApprovalRequest;
import com.acme.budgeting.v1.CommentVarianceApprovalResponse;
import com.acme.budgeting.v1.DelegateVarianceApprovalRequest;
import com.acme.budgeting.v1.DelegateVarianceApprovalResponse;
import com.acme.budgeting.v1.EscalateVarianceRequest;
import com.acme.budgeting.v1.EscalateVarianceResponse;
import com.acme.budgeting.v1.GetVarianceApprovalStatusRequest;
import com.acme.budgeting.v1.GetVarianceApprovalStatusResponse;
import com.acme.budgeting.v1.ListPendingVariancesRequest;
import com.acme.budgeting.v1.ListPendingVariancesResponse;
import com.acme.budgeting.v1.RecallVarianceRequest;
import com.acme.budgeting.v1.RecallVarianceResponse;
import com.acme.budgeting.v1.SubmitVarianceRequest;
import com.acme.budgeting.v1.SubmitVarianceResponse;
import com.acme.budgeting.v1.VarianceApprovalServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes variances through approval, with escalation and delegation.
 */
@GrpcService
public class VarianceApprovalEndpoint extends VarianceApprovalServiceGrpc.VarianceApprovalServiceImplBase {
    protected final VarianceApprovalApplication application;

    public VarianceApprovalEndpoint(VarianceApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a variance.
     */
    @Override
    public void submitVariance(SubmitVarianceRequest request, StreamObserver<SubmitVarianceResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitVariance(request));
    }

    /**
     * Approves a variance.
     */
    @Override
    public void approveVariance(ApproveVarianceRequest request, StreamObserver<ApproveVarianceResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveVariance(request));
    }

    /**
     * Escalates a variance.
     */
    @Override
    public void escalateVariance(EscalateVarianceRequest request, StreamObserver<EscalateVarianceResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateVariance(request));
    }

    /**
     * Lists variances a page at a time.
     */
    @Override
    public void listPendingVariances(ListPendingVariancesRequest request, StreamObserver<ListPendingVariancesResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingVariances(request));
    }

    /**
     * Delegates a variance.
     */
    @Override
    public void delegateVarianceApproval(DelegateVarianceApprovalRequest request, StreamObserver<DelegateVarianceApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateVarianceApproval(request));
    }

    /**
     * Recalls a variance.
     */
    @Override
    public void recallVariance(RecallVarianceRequest request, StreamObserver<RecallVarianceResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallVariance(request));
    }

    /**
     * Returns a variance.
     */
    @Override
    public void getVarianceApprovalStatus(GetVarianceApprovalStatusRequest request, StreamObserver<GetVarianceApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getVarianceApprovalStatus(request));
    }

    /**
     * Comments a variance.
     */
    @Override
    public void commentVarianceApproval(CommentVarianceApprovalRequest request, StreamObserver<CommentVarianceApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentVarianceApproval(request));
    }
}
