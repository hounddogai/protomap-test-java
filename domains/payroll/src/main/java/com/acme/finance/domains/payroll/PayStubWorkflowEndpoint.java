package com.acme.finance.domains.payroll;

import com.acme.finance.platform.RpcSupport;
import com.acme.payroll.v1.AdvancePayStubWorkflowRequest;
import com.acme.payroll.v1.AdvancePayStubWorkflowResponse;
import com.acme.payroll.v1.CancelPayStubWorkflowRequest;
import com.acme.payroll.v1.CancelPayStubWorkflowResponse;
import com.acme.payroll.v1.InspectPayStubWorkflowRequest;
import com.acme.payroll.v1.InspectPayStubWorkflowResponse;
import com.acme.payroll.v1.PausePayStubWorkflowRequest;
import com.acme.payroll.v1.PausePayStubWorkflowResponse;
import com.acme.payroll.v1.PayStubWorkflowServiceGrpc;
import com.acme.payroll.v1.ResumePayStubWorkflowRequest;
import com.acme.payroll.v1.ResumePayStubWorkflowResponse;
import com.acme.payroll.v1.RetryPayStubWorkflowRequest;
import com.acme.payroll.v1.RetryPayStubWorkflowResponse;
import com.acme.payroll.v1.SignalPayStubWorkflowRequest;
import com.acme.payroll.v1.SignalPayStubWorkflowResponse;
import com.acme.payroll.v1.StartPayStubWorkflowRequest;
import com.acme.payroll.v1.StartPayStubWorkflowResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes a pay stub from start to finish.
 */
@GrpcService
public class PayStubWorkflowEndpoint extends PayStubWorkflowServiceGrpc.PayStubWorkflowServiceImplBase {
    protected final PayStubWorkflowApplication application;

    public PayStubWorkflowEndpoint(PayStubWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a pay stub.
     */
    @Override
    public void startPayStubWorkflow(StartPayStubWorkflowRequest request, StreamObserver<StartPayStubWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startPayStubWorkflow(request));
    }

    /**
     * Advances a pay stub.
     */
    @Override
    public void advancePayStubWorkflow(AdvancePayStubWorkflowRequest request, StreamObserver<AdvancePayStubWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advancePayStubWorkflow(request));
    }

    /**
     * Cancels a pay stub.
     */
    @Override
    public void cancelPayStubWorkflow(CancelPayStubWorkflowRequest request, StreamObserver<CancelPayStubWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelPayStubWorkflow(request));
    }

    /**
     * Retrys a pay stub.
     */
    @Override
    public void retryPayStubWorkflow(RetryPayStubWorkflowRequest request, StreamObserver<RetryPayStubWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryPayStubWorkflow(request));
    }

    /**
     * Pauses a pay stub.
     */
    @Override
    public void pausePayStubWorkflow(PausePayStubWorkflowRequest request, StreamObserver<PausePayStubWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pausePayStubWorkflow(request));
    }

    /**
     * Resumes a pay stub.
     */
    @Override
    public void resumePayStubWorkflow(ResumePayStubWorkflowRequest request, StreamObserver<ResumePayStubWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumePayStubWorkflow(request));
    }

    /**
     * Returns a pay stub.
     */
    @Override
    public void inspectPayStubWorkflow(InspectPayStubWorkflowRequest request, StreamObserver<InspectPayStubWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectPayStubWorkflow(request));
    }

    /**
     * Signals a pay stub.
     */
    @Override
    public void signalPayStubWorkflow(SignalPayStubWorkflowRequest request, StreamObserver<SignalPayStubWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalPayStubWorkflow(request));
    }
}
