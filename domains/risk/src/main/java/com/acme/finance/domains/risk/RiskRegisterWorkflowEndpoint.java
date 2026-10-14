package com.acme.finance.domains.risk;

import com.acme.finance.platform.RpcSupport;
import com.acme.risk.v1.AdvanceRiskRegisterWorkflowRequest;
import com.acme.risk.v1.AdvanceRiskRegisterWorkflowResponse;
import com.acme.risk.v1.CancelRiskRegisterWorkflowRequest;
import com.acme.risk.v1.CancelRiskRegisterWorkflowResponse;
import com.acme.risk.v1.InspectRiskRegisterWorkflowRequest;
import com.acme.risk.v1.InspectRiskRegisterWorkflowResponse;
import com.acme.risk.v1.PauseRiskRegisterWorkflowRequest;
import com.acme.risk.v1.PauseRiskRegisterWorkflowResponse;
import com.acme.risk.v1.ResumeRiskRegisterWorkflowRequest;
import com.acme.risk.v1.ResumeRiskRegisterWorkflowResponse;
import com.acme.risk.v1.RetryRiskRegisterWorkflowRequest;
import com.acme.risk.v1.RetryRiskRegisterWorkflowResponse;
import com.acme.risk.v1.RiskRegisterWorkflowServiceGrpc;
import com.acme.risk.v1.SignalRiskRegisterWorkflowRequest;
import com.acme.risk.v1.SignalRiskRegisterWorkflowResponse;
import com.acme.risk.v1.StartRiskRegisterWorkflowRequest;
import com.acme.risk.v1.StartRiskRegisterWorkflowResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes a risk register from start to finish.
 */
@GrpcService
public class RiskRegisterWorkflowEndpoint extends RiskRegisterWorkflowServiceGrpc.RiskRegisterWorkflowServiceImplBase {
    protected final RiskRegisterWorkflowApplication application;

    public RiskRegisterWorkflowEndpoint(RiskRegisterWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a risk register.
     */
    @Override
    public void startRiskRegisterWorkflow(StartRiskRegisterWorkflowRequest request, StreamObserver<StartRiskRegisterWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startRiskRegisterWorkflow(request));
    }

    /**
     * Advances a risk register.
     */
    @Override
    public void advanceRiskRegisterWorkflow(AdvanceRiskRegisterWorkflowRequest request, StreamObserver<AdvanceRiskRegisterWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceRiskRegisterWorkflow(request));
    }

    /**
     * Cancels a risk register.
     */
    @Override
    public void cancelRiskRegisterWorkflow(CancelRiskRegisterWorkflowRequest request, StreamObserver<CancelRiskRegisterWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelRiskRegisterWorkflow(request));
    }

    /**
     * Retrys a risk register.
     */
    @Override
    public void retryRiskRegisterWorkflow(RetryRiskRegisterWorkflowRequest request, StreamObserver<RetryRiskRegisterWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryRiskRegisterWorkflow(request));
    }

    /**
     * Pauses a risk register.
     */
    @Override
    public void pauseRiskRegisterWorkflow(PauseRiskRegisterWorkflowRequest request, StreamObserver<PauseRiskRegisterWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseRiskRegisterWorkflow(request));
    }

    /**
     * Resumes a risk register.
     */
    @Override
    public void resumeRiskRegisterWorkflow(ResumeRiskRegisterWorkflowRequest request, StreamObserver<ResumeRiskRegisterWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeRiskRegisterWorkflow(request));
    }

    /**
     * Returns a risk register.
     */
    @Override
    public void inspectRiskRegisterWorkflow(InspectRiskRegisterWorkflowRequest request, StreamObserver<InspectRiskRegisterWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectRiskRegisterWorkflow(request));
    }

    /**
     * Signals a risk register.
     */
    @Override
    public void signalRiskRegisterWorkflow(SignalRiskRegisterWorkflowRequest request, StreamObserver<SignalRiskRegisterWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalRiskRegisterWorkflow(request));
    }
}
