package com.acme.finance.domains.risk;

import com.acme.finance.platform.RpcSupport;
import com.acme.risk.v1.ApproveRiskScenarioRequest;
import com.acme.risk.v1.ApproveRiskScenarioResponse;
import com.acme.risk.v1.CommentRiskScenarioApprovalRequest;
import com.acme.risk.v1.CommentRiskScenarioApprovalResponse;
import com.acme.risk.v1.DelegateRiskScenarioApprovalRequest;
import com.acme.risk.v1.DelegateRiskScenarioApprovalResponse;
import com.acme.risk.v1.EscalateRiskScenarioRequest;
import com.acme.risk.v1.EscalateRiskScenarioResponse;
import com.acme.risk.v1.ListPendingRiskScenariosRequest;
import com.acme.risk.v1.ListPendingRiskScenariosResponse;
import com.acme.risk.v1.RecallRiskScenarioRequest;
import com.acme.risk.v1.RecallRiskScenarioResponse;
import com.acme.risk.v1.RejectRiskScenarioRequest;
import com.acme.risk.v1.RejectRiskScenarioResponse;
import com.acme.risk.v1.RiskScenarioApprovalServiceGrpc;
import com.acme.risk.v1.SubmitRiskScenarioRequest;
import com.acme.risk.v1.SubmitRiskScenarioResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes risk scenarios through approval, with escalation and delegation.
 */
@GrpcService
public class RiskScenarioApprovalEndpoint extends RiskScenarioApprovalServiceGrpc.RiskScenarioApprovalServiceImplBase {
    protected final RiskScenarioApprovalApplication application;

    public RiskScenarioApprovalEndpoint(RiskScenarioApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a risk scenario.
     */
    @Override
    public void submitRiskScenario(SubmitRiskScenarioRequest request, StreamObserver<SubmitRiskScenarioResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitRiskScenario(request));
    }

    /**
     * Approves a risk scenario.
     */
    @Override
    public void approveRiskScenario(ApproveRiskScenarioRequest request, StreamObserver<ApproveRiskScenarioResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveRiskScenario(request));
    }

    /**
     * Rejects a risk scenario.
     */
    @Override
    public void rejectRiskScenario(RejectRiskScenarioRequest request, StreamObserver<RejectRiskScenarioResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectRiskScenario(request));
    }

    /**
     * Escalates a risk scenario.
     */
    @Override
    public void escalateRiskScenario(EscalateRiskScenarioRequest request, StreamObserver<EscalateRiskScenarioResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateRiskScenario(request));
    }

    /**
     * Lists risk scenarios a page at a time.
     */
    @Override
    public void listPendingRiskScenarios(ListPendingRiskScenariosRequest request, StreamObserver<ListPendingRiskScenariosResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingRiskScenarios(request));
    }

    /**
     * Delegates a risk scenario.
     */
    @Override
    public void delegateRiskScenarioApproval(DelegateRiskScenarioApprovalRequest request, StreamObserver<DelegateRiskScenarioApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateRiskScenarioApproval(request));
    }

    /**
     * Recalls a risk scenario.
     */
    @Override
    public void recallRiskScenario(RecallRiskScenarioRequest request, StreamObserver<RecallRiskScenarioResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallRiskScenario(request));
    }

    /**
     * Comments a risk scenario.
     */
    @Override
    public void commentRiskScenarioApproval(CommentRiskScenarioApprovalRequest request, StreamObserver<CommentRiskScenarioApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentRiskScenarioApproval(request));
    }
}
