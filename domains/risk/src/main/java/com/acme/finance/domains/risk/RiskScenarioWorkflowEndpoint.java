package com.acme.finance.domains.risk;

import com.acme.finance.platform.RpcSupport;
import com.acme.risk.v1.AdvanceRiskScenarioWorkflowRequest;
import com.acme.risk.v1.AdvanceRiskScenarioWorkflowResponse;
import com.acme.risk.v1.CancelRiskScenarioWorkflowRequest;
import com.acme.risk.v1.CancelRiskScenarioWorkflowResponse;
import com.acme.risk.v1.InspectRiskScenarioWorkflowRequest;
import com.acme.risk.v1.InspectRiskScenarioWorkflowResponse;
import com.acme.risk.v1.ListRiskScenarioWorkflowsRequest;
import com.acme.risk.v1.ListRiskScenarioWorkflowsResponse;
import com.acme.risk.v1.RetryRiskScenarioWorkflowRequest;
import com.acme.risk.v1.RetryRiskScenarioWorkflowResponse;
import com.acme.risk.v1.RiskScenarioWorkflowServiceGrpc;
import com.acme.risk.v1.SignalRiskScenarioWorkflowRequest;
import com.acme.risk.v1.SignalRiskScenarioWorkflowResponse;
import com.acme.risk.v1.StartRiskScenarioWorkflowRequest;
import com.acme.risk.v1.StartRiskScenarioWorkflowResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes a risk scenario from start to finish.
 */
@GrpcService
public class RiskScenarioWorkflowEndpoint extends RiskScenarioWorkflowServiceGrpc.RiskScenarioWorkflowServiceImplBase {
    protected final RiskScenarioWorkflowApplication application;

    public RiskScenarioWorkflowEndpoint(RiskScenarioWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a risk scenario.
     */
    @Override
    public void startRiskScenarioWorkflow(StartRiskScenarioWorkflowRequest request, StreamObserver<StartRiskScenarioWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startRiskScenarioWorkflow(request));
    }

    /**
     * Advances a risk scenario.
     */
    @Override
    public void advanceRiskScenarioWorkflow(AdvanceRiskScenarioWorkflowRequest request, StreamObserver<AdvanceRiskScenarioWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceRiskScenarioWorkflow(request));
    }

    /**
     * Cancels a risk scenario.
     */
    @Override
    public void cancelRiskScenarioWorkflow(CancelRiskScenarioWorkflowRequest request, StreamObserver<CancelRiskScenarioWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelRiskScenarioWorkflow(request));
    }

    /**
     * Retrys a risk scenario.
     */
    @Override
    public void retryRiskScenarioWorkflow(RetryRiskScenarioWorkflowRequest request, StreamObserver<RetryRiskScenarioWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryRiskScenarioWorkflow(request));
    }

    /**
     * Returns a risk scenario.
     */
    @Override
    public void inspectRiskScenarioWorkflow(InspectRiskScenarioWorkflowRequest request, StreamObserver<InspectRiskScenarioWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectRiskScenarioWorkflow(request));
    }

    /**
     * Lists risk scenarios a page at a time.
     */
    @Override
    public void listRiskScenarioWorkflows(ListRiskScenarioWorkflowsRequest request, StreamObserver<ListRiskScenarioWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listRiskScenarioWorkflows(request));
    }

    /**
     * Signals a risk scenario.
     */
    @Override
    public void signalRiskScenarioWorkflow(SignalRiskScenarioWorkflowRequest request, StreamObserver<SignalRiskScenarioWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalRiskScenarioWorkflow(request));
    }
}
