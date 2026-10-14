package com.acme.finance.domains.assets;

import com.acme.assets.v1.ApproveDepreciationRequest;
import com.acme.assets.v1.ApproveDepreciationResponse;
import com.acme.assets.v1.DelegateDepreciationApprovalRequest;
import com.acme.assets.v1.DelegateDepreciationApprovalResponse;
import com.acme.assets.v1.DepreciationApprovalServiceGrpc;
import com.acme.assets.v1.EscalateDepreciationRequest;
import com.acme.assets.v1.EscalateDepreciationResponse;
import com.acme.assets.v1.GetDepreciationApprovalStatusRequest;
import com.acme.assets.v1.GetDepreciationApprovalStatusResponse;
import com.acme.assets.v1.ListPendingDepreciationsRequest;
import com.acme.assets.v1.ListPendingDepreciationsResponse;
import com.acme.assets.v1.RecallDepreciationRequest;
import com.acme.assets.v1.RecallDepreciationResponse;
import com.acme.assets.v1.RejectDepreciationRequest;
import com.acme.assets.v1.RejectDepreciationResponse;
import com.acme.assets.v1.SubmitDepreciationRequest;
import com.acme.assets.v1.SubmitDepreciationResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes depreciations through approval, with escalation and delegation.
 */
@GrpcService
public class DepreciationApprovalEndpoint extends DepreciationApprovalServiceGrpc.DepreciationApprovalServiceImplBase {
    protected final DepreciationApprovalApplication application;

    public DepreciationApprovalEndpoint(DepreciationApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a depreciation.
     */
    @Override
    public void submitDepreciation(SubmitDepreciationRequest request, StreamObserver<SubmitDepreciationResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitDepreciation(request));
    }

    /**
     * Approves a depreciation.
     */
    @Override
    public void approveDepreciation(ApproveDepreciationRequest request, StreamObserver<ApproveDepreciationResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveDepreciation(request));
    }

    /**
     * Rejects a depreciation.
     */
    @Override
    public void rejectDepreciation(RejectDepreciationRequest request, StreamObserver<RejectDepreciationResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectDepreciation(request));
    }

    /**
     * Escalates a depreciation.
     */
    @Override
    public void escalateDepreciation(EscalateDepreciationRequest request, StreamObserver<EscalateDepreciationResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateDepreciation(request));
    }

    /**
     * Lists depreciations a page at a time.
     */
    @Override
    public void listPendingDepreciations(ListPendingDepreciationsRequest request, StreamObserver<ListPendingDepreciationsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingDepreciations(request));
    }

    /**
     * Delegates a depreciation.
     */
    @Override
    public void delegateDepreciationApproval(DelegateDepreciationApprovalRequest request, StreamObserver<DelegateDepreciationApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateDepreciationApproval(request));
    }

    /**
     * Recalls a depreciation.
     */
    @Override
    public void recallDepreciation(RecallDepreciationRequest request, StreamObserver<RecallDepreciationResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallDepreciation(request));
    }

    /**
     * Returns a depreciation.
     */
    @Override
    public void getDepreciationApprovalStatus(GetDepreciationApprovalStatusRequest request, StreamObserver<GetDepreciationApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getDepreciationApprovalStatus(request));
    }
}
