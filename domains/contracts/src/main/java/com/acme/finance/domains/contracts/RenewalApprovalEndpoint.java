package com.acme.finance.domains.contracts;

import com.acme.contracts.v1.ApproveRenewalRequest;
import com.acme.contracts.v1.ApproveRenewalResponse;
import com.acme.contracts.v1.DelegateRenewalApprovalRequest;
import com.acme.contracts.v1.DelegateRenewalApprovalResponse;
import com.acme.contracts.v1.GetRenewalApprovalStatusRequest;
import com.acme.contracts.v1.GetRenewalApprovalStatusResponse;
import com.acme.contracts.v1.RecallRenewalRequest;
import com.acme.contracts.v1.RecallRenewalResponse;
import com.acme.contracts.v1.RejectRenewalRequest;
import com.acme.contracts.v1.RejectRenewalResponse;
import com.acme.contracts.v1.RenewalApprovalServiceGrpc;
import com.acme.contracts.v1.SubmitRenewalRequest;
import com.acme.contracts.v1.SubmitRenewalResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes renewals through approval, with escalation and delegation.
 */
@GrpcService
public class RenewalApprovalEndpoint extends RenewalApprovalServiceGrpc.RenewalApprovalServiceImplBase {
    protected final RenewalApprovalApplication application;

    public RenewalApprovalEndpoint(RenewalApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a renewal.
     */
    @Override
    public void submitRenewal(SubmitRenewalRequest request, StreamObserver<SubmitRenewalResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitRenewal(request));
    }

    /**
     * Approves a renewal.
     */
    @Override
    public void approveRenewal(ApproveRenewalRequest request, StreamObserver<ApproveRenewalResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveRenewal(request));
    }

    /**
     * Rejects a renewal.
     */
    @Override
    public void rejectRenewal(RejectRenewalRequest request, StreamObserver<RejectRenewalResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectRenewal(request));
    }

    /**
     * Delegates a renewal.
     */
    @Override
    public void delegateRenewalApproval(DelegateRenewalApprovalRequest request, StreamObserver<DelegateRenewalApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateRenewalApproval(request));
    }

    /**
     * Recalls a renewal.
     */
    @Override
    public void recallRenewal(RecallRenewalRequest request, StreamObserver<RecallRenewalResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallRenewal(request));
    }

    /**
     * Returns a renewal.
     */
    @Override
    public void getRenewalApprovalStatus(GetRenewalApprovalStatusRequest request, StreamObserver<GetRenewalApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getRenewalApprovalStatus(request));
    }
}
