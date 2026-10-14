package com.acme.finance.domains.banking;

import com.acme.banking.v1.AdvanceStandingOrderWorkflowRequest;
import com.acme.banking.v1.AdvanceStandingOrderWorkflowResponse;
import com.acme.banking.v1.CancelStandingOrderWorkflowRequest;
import com.acme.banking.v1.CancelStandingOrderWorkflowResponse;
import com.acme.banking.v1.InspectStandingOrderWorkflowRequest;
import com.acme.banking.v1.InspectStandingOrderWorkflowResponse;
import com.acme.banking.v1.ListStandingOrderWorkflowsRequest;
import com.acme.banking.v1.ListStandingOrderWorkflowsResponse;
import com.acme.banking.v1.PauseStandingOrderWorkflowRequest;
import com.acme.banking.v1.PauseStandingOrderWorkflowResponse;
import com.acme.banking.v1.ResumeStandingOrderWorkflowRequest;
import com.acme.banking.v1.ResumeStandingOrderWorkflowResponse;
import com.acme.banking.v1.RetryStandingOrderWorkflowRequest;
import com.acme.banking.v1.RetryStandingOrderWorkflowResponse;
import com.acme.banking.v1.SignalStandingOrderWorkflowRequest;
import com.acme.banking.v1.SignalStandingOrderWorkflowResponse;
import com.acme.banking.v1.StandingOrderWorkflowServiceGrpc;
import com.acme.banking.v1.StartStandingOrderWorkflowRequest;
import com.acme.banking.v1.StartStandingOrderWorkflowResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes a standing order from start to finish.
 */
@GrpcService
public class StandingOrderWorkflowEndpoint extends StandingOrderWorkflowServiceGrpc.StandingOrderWorkflowServiceImplBase {
    protected final StandingOrderWorkflowApplication application;

    public StandingOrderWorkflowEndpoint(StandingOrderWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a standing order.
     */
    @Override
    public void startStandingOrderWorkflow(StartStandingOrderWorkflowRequest request, StreamObserver<StartStandingOrderWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startStandingOrderWorkflow(request));
    }

    /**
     * Advances a standing order.
     */
    @Override
    public void advanceStandingOrderWorkflow(AdvanceStandingOrderWorkflowRequest request, StreamObserver<AdvanceStandingOrderWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceStandingOrderWorkflow(request));
    }

    /**
     * Cancels a standing order.
     */
    @Override
    public void cancelStandingOrderWorkflow(CancelStandingOrderWorkflowRequest request, StreamObserver<CancelStandingOrderWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelStandingOrderWorkflow(request));
    }

    /**
     * Retrys a standing order.
     */
    @Override
    public void retryStandingOrderWorkflow(RetryStandingOrderWorkflowRequest request, StreamObserver<RetryStandingOrderWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryStandingOrderWorkflow(request));
    }

    /**
     * Pauses a standing order.
     */
    @Override
    public void pauseStandingOrderWorkflow(PauseStandingOrderWorkflowRequest request, StreamObserver<PauseStandingOrderWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseStandingOrderWorkflow(request));
    }

    /**
     * Resumes a standing order.
     */
    @Override
    public void resumeStandingOrderWorkflow(ResumeStandingOrderWorkflowRequest request, StreamObserver<ResumeStandingOrderWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeStandingOrderWorkflow(request));
    }

    /**
     * Returns a standing order.
     */
    @Override
    public void inspectStandingOrderWorkflow(InspectStandingOrderWorkflowRequest request, StreamObserver<InspectStandingOrderWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectStandingOrderWorkflow(request));
    }

    /**
     * Lists standing orders a page at a time.
     */
    @Override
    public void listStandingOrderWorkflows(ListStandingOrderWorkflowsRequest request, StreamObserver<ListStandingOrderWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listStandingOrderWorkflows(request));
    }

    /**
     * Signals a standing order.
     */
    @Override
    public void signalStandingOrderWorkflow(SignalStandingOrderWorkflowRequest request, StreamObserver<SignalStandingOrderWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalStandingOrderWorkflow(request));
    }
}
