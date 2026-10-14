package com.acme.finance.domains.collections;

import com.acme.collections.v1.AdvanceWriteOffWorkflowRequest;
import com.acme.collections.v1.AdvanceWriteOffWorkflowResponse;
import com.acme.collections.v1.CancelWriteOffWorkflowRequest;
import com.acme.collections.v1.CancelWriteOffWorkflowResponse;
import com.acme.collections.v1.InspectWriteOffWorkflowRequest;
import com.acme.collections.v1.InspectWriteOffWorkflowResponse;
import com.acme.collections.v1.ListWriteOffWorkflowsRequest;
import com.acme.collections.v1.ListWriteOffWorkflowsResponse;
import com.acme.collections.v1.PauseWriteOffWorkflowRequest;
import com.acme.collections.v1.PauseWriteOffWorkflowResponse;
import com.acme.collections.v1.ResumeWriteOffWorkflowRequest;
import com.acme.collections.v1.ResumeWriteOffWorkflowResponse;
import com.acme.collections.v1.RetryWriteOffWorkflowRequest;
import com.acme.collections.v1.RetryWriteOffWorkflowResponse;
import com.acme.collections.v1.SignalWriteOffWorkflowRequest;
import com.acme.collections.v1.SignalWriteOffWorkflowResponse;
import com.acme.collections.v1.StartWriteOffWorkflowRequest;
import com.acme.collections.v1.StartWriteOffWorkflowResponse;
import com.acme.collections.v1.WriteOffWorkflowServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes a write off from start to finish.
 */
@GrpcService
public class WriteOffWorkflowEndpoint extends WriteOffWorkflowServiceGrpc.WriteOffWorkflowServiceImplBase {
    protected final WriteOffWorkflowApplication application;

    public WriteOffWorkflowEndpoint(WriteOffWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a write off.
     */
    @Override
    public void startWriteOffWorkflow(StartWriteOffWorkflowRequest request, StreamObserver<StartWriteOffWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startWriteOffWorkflow(request));
    }

    /**
     * Advances a write off.
     */
    @Override
    public void advanceWriteOffWorkflow(AdvanceWriteOffWorkflowRequest request, StreamObserver<AdvanceWriteOffWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceWriteOffWorkflow(request));
    }

    /**
     * Cancels a write off.
     */
    @Override
    public void cancelWriteOffWorkflow(CancelWriteOffWorkflowRequest request, StreamObserver<CancelWriteOffWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelWriteOffWorkflow(request));
    }

    /**
     * Retrys a write off.
     */
    @Override
    public void retryWriteOffWorkflow(RetryWriteOffWorkflowRequest request, StreamObserver<RetryWriteOffWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryWriteOffWorkflow(request));
    }

    /**
     * Pauses a write off.
     */
    @Override
    public void pauseWriteOffWorkflow(PauseWriteOffWorkflowRequest request, StreamObserver<PauseWriteOffWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseWriteOffWorkflow(request));
    }

    /**
     * Resumes a write off.
     */
    @Override
    public void resumeWriteOffWorkflow(ResumeWriteOffWorkflowRequest request, StreamObserver<ResumeWriteOffWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeWriteOffWorkflow(request));
    }

    /**
     * Returns a write off.
     */
    @Override
    public void inspectWriteOffWorkflow(InspectWriteOffWorkflowRequest request, StreamObserver<InspectWriteOffWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectWriteOffWorkflow(request));
    }

    /**
     * Lists write offs a page at a time.
     */
    @Override
    public void listWriteOffWorkflows(ListWriteOffWorkflowsRequest request, StreamObserver<ListWriteOffWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listWriteOffWorkflows(request));
    }

    /**
     * Signals a write off.
     */
    @Override
    public void signalWriteOffWorkflow(SignalWriteOffWorkflowRequest request, StreamObserver<SignalWriteOffWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalWriteOffWorkflow(request));
    }
}
