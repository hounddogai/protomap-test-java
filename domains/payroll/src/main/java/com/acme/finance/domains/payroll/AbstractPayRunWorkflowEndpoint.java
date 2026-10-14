package com.acme.finance.domains.payroll;

import com.acme.finance.platform.RpcSupport;
import com.acme.payroll.v1.AdvancePayRunWorkflowRequest;
import com.acme.payroll.v1.AdvancePayRunWorkflowResponse;
import com.acme.payroll.v1.CancelPayRunWorkflowRequest;
import com.acme.payroll.v1.CancelPayRunWorkflowResponse;
import com.acme.payroll.v1.InspectPayRunWorkflowRequest;
import com.acme.payroll.v1.InspectPayRunWorkflowResponse;
import com.acme.payroll.v1.ListPayRunWorkflowsRequest;
import com.acme.payroll.v1.ListPayRunWorkflowsResponse;
import com.acme.payroll.v1.PausePayRunWorkflowRequest;
import com.acme.payroll.v1.PausePayRunWorkflowResponse;
import com.acme.payroll.v1.PayRunWorkflowServiceGrpc;
import com.acme.payroll.v1.ResumePayRunWorkflowRequest;
import com.acme.payroll.v1.ResumePayRunWorkflowResponse;
import com.acme.payroll.v1.RetryPayRunWorkflowRequest;
import com.acme.payroll.v1.RetryPayRunWorkflowResponse;
import com.acme.payroll.v1.SignalPayRunWorkflowRequest;
import com.acme.payroll.v1.SignalPayRunWorkflowResponse;
import com.acme.payroll.v1.StartPayRunWorkflowRequest;
import com.acme.payroll.v1.StartPayRunWorkflowResponse;
import io.grpc.stub.StreamObserver;

/**
 * Runs the long-lived workflow that takes a pay run from start to finish.
 */
public abstract class AbstractPayRunWorkflowEndpoint extends PayRunWorkflowServiceGrpc.PayRunWorkflowServiceImplBase {
    protected final PayRunWorkflowApplication application;

    protected AbstractPayRunWorkflowEndpoint(PayRunWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a pay run.
     */
    @Override
    public void startPayRunWorkflow(StartPayRunWorkflowRequest request, StreamObserver<StartPayRunWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startPayRunWorkflow(request));
    }

    /**
     * Advances a pay run.
     */
    @Override
    public void advancePayRunWorkflow(AdvancePayRunWorkflowRequest request, StreamObserver<AdvancePayRunWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advancePayRunWorkflow(request));
    }

    /**
     * Cancels a pay run.
     */
    @Override
    public void cancelPayRunWorkflow(CancelPayRunWorkflowRequest request, StreamObserver<CancelPayRunWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelPayRunWorkflow(request));
    }

    /**
     * Retrys a pay run.
     */
    @Override
    public void retryPayRunWorkflow(RetryPayRunWorkflowRequest request, StreamObserver<RetryPayRunWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryPayRunWorkflow(request));
    }

    /**
     * Pauses a pay run.
     */
    @Override
    public void pausePayRunWorkflow(PausePayRunWorkflowRequest request, StreamObserver<PausePayRunWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pausePayRunWorkflow(request));
    }

    /**
     * Resumes a pay run.
     */
    @Override
    public void resumePayRunWorkflow(ResumePayRunWorkflowRequest request, StreamObserver<ResumePayRunWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumePayRunWorkflow(request));
    }

    /**
     * Returns a pay run.
     */
    @Override
    public void inspectPayRunWorkflow(InspectPayRunWorkflowRequest request, StreamObserver<InspectPayRunWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectPayRunWorkflow(request));
    }

    /**
     * Lists pay runs a page at a time.
     */
    @Override
    public void listPayRunWorkflows(ListPayRunWorkflowsRequest request, StreamObserver<ListPayRunWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPayRunWorkflows(request));
    }

    /**
     * Signals a pay run.
     */
    @Override
    public void signalPayRunWorkflow(SignalPayRunWorkflowRequest request, StreamObserver<SignalPayRunWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalPayRunWorkflow(request));
    }
}
