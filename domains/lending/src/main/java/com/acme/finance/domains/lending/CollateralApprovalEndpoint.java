package com.acme.finance.domains.lending;

import com.acme.finance.platform.RpcSupport;
import com.acme.lending.v1.ApproveCollateralRequest;
import com.acme.lending.v1.ApproveCollateralResponse;
import com.acme.lending.v1.CollateralApprovalServiceGrpc;
import com.acme.lending.v1.CommentCollateralApprovalRequest;
import com.acme.lending.v1.CommentCollateralApprovalResponse;
import com.acme.lending.v1.DelegateCollateralApprovalRequest;
import com.acme.lending.v1.DelegateCollateralApprovalResponse;
import com.acme.lending.v1.EscalateCollateralRequest;
import com.acme.lending.v1.EscalateCollateralResponse;
import com.acme.lending.v1.GetCollateralApprovalStatusRequest;
import com.acme.lending.v1.GetCollateralApprovalStatusResponse;
import com.acme.lending.v1.ListPendingCollateralsRequest;
import com.acme.lending.v1.ListPendingCollateralsResponse;
import com.acme.lending.v1.RecallCollateralRequest;
import com.acme.lending.v1.RecallCollateralResponse;
import com.acme.lending.v1.RejectCollateralRequest;
import com.acme.lending.v1.RejectCollateralResponse;
import com.acme.lending.v1.SubmitCollateralRequest;
import com.acme.lending.v1.SubmitCollateralResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes collaterals through approval, with escalation and delegation.
 */
@GrpcService
public class CollateralApprovalEndpoint extends CollateralApprovalServiceGrpc.CollateralApprovalServiceImplBase {
    protected final CollateralApprovalApplication application;

    public CollateralApprovalEndpoint(CollateralApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a collateral.
     */
    @Override
    public void submitCollateral(SubmitCollateralRequest request, StreamObserver<SubmitCollateralResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitCollateral(request));
    }

    /**
     * Approves a collateral.
     */
    @Override
    public void approveCollateral(ApproveCollateralRequest request, StreamObserver<ApproveCollateralResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveCollateral(request));
    }

    /**
     * Rejects a collateral.
     */
    @Override
    public void rejectCollateral(RejectCollateralRequest request, StreamObserver<RejectCollateralResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectCollateral(request));
    }

    /**
     * Escalates a collateral.
     */
    @Override
    public void escalateCollateral(EscalateCollateralRequest request, StreamObserver<EscalateCollateralResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateCollateral(request));
    }

    /**
     * Lists collaterals a page at a time.
     */
    @Override
    public void listPendingCollaterals(ListPendingCollateralsRequest request, StreamObserver<ListPendingCollateralsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingCollaterals(request));
    }

    /**
     * Delegates a collateral.
     */
    @Override
    public void delegateCollateralApproval(DelegateCollateralApprovalRequest request, StreamObserver<DelegateCollateralApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateCollateralApproval(request));
    }

    /**
     * Recalls a collateral.
     */
    @Override
    public void recallCollateral(RecallCollateralRequest request, StreamObserver<RecallCollateralResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallCollateral(request));
    }

    /**
     * Returns a collateral.
     */
    @Override
    public void getCollateralApprovalStatus(GetCollateralApprovalStatusRequest request, StreamObserver<GetCollateralApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getCollateralApprovalStatus(request));
    }

    /**
     * Comments a collateral.
     */
    @Override
    public void commentCollateralApproval(CommentCollateralApprovalRequest request, StreamObserver<CommentCollateralApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentCollateralApproval(request));
    }
}
