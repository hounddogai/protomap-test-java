package com.acme.finance.domains.risk;

import com.acme.finance.platform.RpcSupport;
import com.acme.risk.v1.AdvanceExposureWorkflowRequest;
import com.acme.risk.v1.AdvanceExposureWorkflowResponse;
import com.acme.risk.v1.CancelExposureWorkflowRequest;
import com.acme.risk.v1.CancelExposureWorkflowResponse;
import com.acme.risk.v1.ExposureWorkflowServiceGrpc;
import com.acme.risk.v1.InspectExposureWorkflowRequest;
import com.acme.risk.v1.InspectExposureWorkflowResponse;
import com.acme.risk.v1.PauseExposureWorkflowRequest;
import com.acme.risk.v1.PauseExposureWorkflowResponse;
import com.acme.risk.v1.ResumeExposureWorkflowRequest;
import com.acme.risk.v1.ResumeExposureWorkflowResponse;
import com.acme.risk.v1.SignalExposureWorkflowRequest;
import com.acme.risk.v1.SignalExposureWorkflowResponse;
import com.acme.risk.v1.StartExposureWorkflowRequest;
import com.acme.risk.v1.StartExposureWorkflowResponse;
import io.grpc.stub.StreamObserver;

/**
 * Runs the long-lived workflow that takes an exposure from start to finish.
 */
public abstract class AbstractExposureWorkflowEndpoint extends ExposureWorkflowServiceGrpc.ExposureWorkflowServiceImplBase {
    protected final ExposureWorkflowApplication application;

    protected AbstractExposureWorkflowEndpoint(ExposureWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts an exposure.
     */
    @Override
    public void startExposureWorkflow(StartExposureWorkflowRequest request, StreamObserver<StartExposureWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startExposureWorkflow(request));
    }

    /**
     * Advances an exposure.
     */
    @Override
    public void advanceExposureWorkflow(AdvanceExposureWorkflowRequest request, StreamObserver<AdvanceExposureWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceExposureWorkflow(request));
    }

    /**
     * Cancels an exposure.
     */
    @Override
    public void cancelExposureWorkflow(CancelExposureWorkflowRequest request, StreamObserver<CancelExposureWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelExposureWorkflow(request));
    }

    /**
     * Pauses an exposure.
     */
    @Override
    public void pauseExposureWorkflow(PauseExposureWorkflowRequest request, StreamObserver<PauseExposureWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseExposureWorkflow(request));
    }

    /**
     * Resumes an exposure.
     */
    @Override
    public void resumeExposureWorkflow(ResumeExposureWorkflowRequest request, StreamObserver<ResumeExposureWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeExposureWorkflow(request));
    }

    /**
     * Returns an exposure.
     */
    @Override
    public void inspectExposureWorkflow(InspectExposureWorkflowRequest request, StreamObserver<InspectExposureWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectExposureWorkflow(request));
    }

    /**
     * Signals an exposure.
     */
    @Override
    public void signalExposureWorkflow(SignalExposureWorkflowRequest request, StreamObserver<SignalExposureWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalExposureWorkflow(request));
    }
}
