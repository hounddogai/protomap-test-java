package com.acme.finance.domains.risk;

import com.acme.finance.platform.RpcSupport;
import com.acme.risk.v1.ApproveRiskRegisterRequest;
import com.acme.risk.v1.ApproveRiskRegisterResponse;
import com.acme.risk.v1.CommentRiskRegisterApprovalRequest;
import com.acme.risk.v1.CommentRiskRegisterApprovalResponse;
import com.acme.risk.v1.DelegateRiskRegisterApprovalRequest;
import com.acme.risk.v1.DelegateRiskRegisterApprovalResponse;
import com.acme.risk.v1.EscalateRiskRegisterRequest;
import com.acme.risk.v1.EscalateRiskRegisterResponse;
import com.acme.risk.v1.GetRiskRegisterApprovalStatusRequest;
import com.acme.risk.v1.GetRiskRegisterApprovalStatusResponse;
import com.acme.risk.v1.ListPendingRiskRegistersRequest;
import com.acme.risk.v1.ListPendingRiskRegistersResponse;
import com.acme.risk.v1.RecallRiskRegisterRequest;
import com.acme.risk.v1.RecallRiskRegisterResponse;
import com.acme.risk.v1.RejectRiskRegisterRequest;
import com.acme.risk.v1.RejectRiskRegisterResponse;
import com.acme.risk.v1.RiskRegisterApprovalServiceGrpc;
import com.acme.risk.v1.SubmitRiskRegisterRequest;
import com.acme.risk.v1.SubmitRiskRegisterResponse;
import io.grpc.stub.StreamObserver;

/**
 * Routes risk registers through approval, with escalation and delegation.
 */
public abstract class AbstractRiskRegisterApprovalEndpoint extends RiskRegisterApprovalServiceGrpc.RiskRegisterApprovalServiceImplBase {
    protected final RiskRegisterApprovalApplication application;

    protected AbstractRiskRegisterApprovalEndpoint(RiskRegisterApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a risk register.
     */
    @Override
    public void submitRiskRegister(SubmitRiskRegisterRequest request, StreamObserver<SubmitRiskRegisterResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitRiskRegister(request));
    }

    /**
     * Approves a risk register.
     */
    @Override
    public void approveRiskRegister(ApproveRiskRegisterRequest request, StreamObserver<ApproveRiskRegisterResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveRiskRegister(request));
    }

    /**
     * Rejects a risk register.
     */
    @Override
    public void rejectRiskRegister(RejectRiskRegisterRequest request, StreamObserver<RejectRiskRegisterResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectRiskRegister(request));
    }

    /**
     * Escalates a risk register.
     */
    @Override
    public void escalateRiskRegister(EscalateRiskRegisterRequest request, StreamObserver<EscalateRiskRegisterResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateRiskRegister(request));
    }

    /**
     * Lists risk registers a page at a time.
     */
    @Override
    public void listPendingRiskRegisters(ListPendingRiskRegistersRequest request, StreamObserver<ListPendingRiskRegistersResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingRiskRegisters(request));
    }

    /**
     * Delegates a risk register.
     */
    @Override
    public void delegateRiskRegisterApproval(DelegateRiskRegisterApprovalRequest request, StreamObserver<DelegateRiskRegisterApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateRiskRegisterApproval(request));
    }

    /**
     * Recalls a risk register.
     */
    @Override
    public void recallRiskRegister(RecallRiskRegisterRequest request, StreamObserver<RecallRiskRegisterResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallRiskRegister(request));
    }

    /**
     * Returns a risk register.
     */
    @Override
    public void getRiskRegisterApprovalStatus(GetRiskRegisterApprovalStatusRequest request, StreamObserver<GetRiskRegisterApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getRiskRegisterApprovalStatus(request));
    }

    /**
     * Comments a risk register.
     */
    @Override
    public void commentRiskRegisterApproval(CommentRiskRegisterApprovalRequest request, StreamObserver<CommentRiskRegisterApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentRiskRegisterApproval(request));
    }
}
