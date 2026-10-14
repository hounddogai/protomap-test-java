package com.acme.finance.domains.compliance;

import com.acme.compliance.v1.AdvanceControlWorkflowRequest;
import com.acme.compliance.v1.AdvanceControlWorkflowResponse;
import com.acme.compliance.v1.CancelControlWorkflowRequest;
import com.acme.compliance.v1.CancelControlWorkflowResponse;
import com.acme.compliance.v1.ControlWorkflowServiceGrpc;
import com.acme.compliance.v1.ResumeControlWorkflowRequest;
import com.acme.compliance.v1.ResumeControlWorkflowResponse;
import com.acme.compliance.v1.RetryControlWorkflowRequest;
import com.acme.compliance.v1.RetryControlWorkflowResponse;
import com.acme.compliance.v1.SignalControlWorkflowRequest;
import com.acme.compliance.v1.SignalControlWorkflowResponse;
import com.acme.compliance.v1.StartControlWorkflowRequest;
import com.acme.compliance.v1.StartControlWorkflowResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Runs the long-lived workflow that takes a control from start to finish.
 */
public abstract class AbstractControlWorkflowEndpoint extends ControlWorkflowServiceGrpc.ControlWorkflowServiceImplBase {
    protected final ControlWorkflowApplication application;

    protected AbstractControlWorkflowEndpoint(ControlWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a control.
     */
    @Override
    public void startControlWorkflow(StartControlWorkflowRequest request, StreamObserver<StartControlWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startControlWorkflow(request));
    }

    /**
     * Advances a control.
     */
    @Override
    public void advanceControlWorkflow(AdvanceControlWorkflowRequest request, StreamObserver<AdvanceControlWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceControlWorkflow(request));
    }

    /**
     * Cancels a control.
     */
    @Override
    public void cancelControlWorkflow(CancelControlWorkflowRequest request, StreamObserver<CancelControlWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelControlWorkflow(request));
    }

    /**
     * Retrys a control.
     */
    @Override
    public void retryControlWorkflow(RetryControlWorkflowRequest request, StreamObserver<RetryControlWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryControlWorkflow(request));
    }

    /**
     * Resumes a control.
     */
    @Override
    public void resumeControlWorkflow(ResumeControlWorkflowRequest request, StreamObserver<ResumeControlWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeControlWorkflow(request));
    }

    /**
     * Signals a control.
     */
    @Override
    public void signalControlWorkflow(SignalControlWorkflowRequest request, StreamObserver<SignalControlWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalControlWorkflow(request));
    }
}
