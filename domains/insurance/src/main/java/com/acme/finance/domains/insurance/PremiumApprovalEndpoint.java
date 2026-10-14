package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.ApprovePremiumRequest;
import com.acme.insurance.v1.ApprovePremiumResponse;
import com.acme.insurance.v1.CommentPremiumApprovalRequest;
import com.acme.insurance.v1.CommentPremiumApprovalResponse;
import com.acme.insurance.v1.DelegatePremiumApprovalRequest;
import com.acme.insurance.v1.DelegatePremiumApprovalResponse;
import com.acme.insurance.v1.EscalatePremiumRequest;
import com.acme.insurance.v1.EscalatePremiumResponse;
import com.acme.insurance.v1.GetPremiumApprovalStatusRequest;
import com.acme.insurance.v1.GetPremiumApprovalStatusResponse;
import com.acme.insurance.v1.PremiumApprovalServiceGrpc;
import com.acme.insurance.v1.RecallPremiumRequest;
import com.acme.insurance.v1.RecallPremiumResponse;
import com.acme.insurance.v1.RejectPremiumRequest;
import com.acme.insurance.v1.RejectPremiumResponse;
import com.acme.insurance.v1.SubmitPremiumRequest;
import com.acme.insurance.v1.SubmitPremiumResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes premiums through approval, with escalation and delegation.
 */
@GrpcService
public class PremiumApprovalEndpoint extends PremiumApprovalServiceGrpc.PremiumApprovalServiceImplBase {
    protected final PremiumApprovalApplication application;

    public PremiumApprovalEndpoint(PremiumApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a premium.
     */
    @Override
    public void submitPremium(SubmitPremiumRequest request, StreamObserver<SubmitPremiumResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitPremium(request));
    }

    /**
     * Approves a premium.
     */
    @Override
    public void approvePremium(ApprovePremiumRequest request, StreamObserver<ApprovePremiumResponse> observer) {
        RpcSupport.unary(observer, () -> application.approvePremium(request));
    }

    /**
     * Rejects a premium.
     */
    @Override
    public void rejectPremium(RejectPremiumRequest request, StreamObserver<RejectPremiumResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectPremium(request));
    }

    /**
     * Escalates a premium.
     */
    @Override
    public void escalatePremium(EscalatePremiumRequest request, StreamObserver<EscalatePremiumResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalatePremium(request));
    }

    /**
     * Delegates a premium.
     */
    @Override
    public void delegatePremiumApproval(DelegatePremiumApprovalRequest request, StreamObserver<DelegatePremiumApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegatePremiumApproval(request));
    }

    /**
     * Recalls a premium.
     */
    @Override
    public void recallPremium(RecallPremiumRequest request, StreamObserver<RecallPremiumResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallPremium(request));
    }

    /**
     * Returns a premium.
     */
    @Override
    public void getPremiumApprovalStatus(GetPremiumApprovalStatusRequest request, StreamObserver<GetPremiumApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getPremiumApprovalStatus(request));
    }

    /**
     * Comments a premium.
     */
    @Override
    public void commentPremiumApproval(CommentPremiumApprovalRequest request, StreamObserver<CommentPremiumApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentPremiumApproval(request));
    }
}
