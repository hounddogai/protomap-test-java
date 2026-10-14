package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.AdvanceAllocationWorkflowRequest;
import com.acme.accounting.v1.AdvanceAllocationWorkflowResponse;
import com.acme.accounting.v1.AllocationWorkflowServiceGrpc;
import com.acme.accounting.v1.CancelAllocationWorkflowRequest;
import com.acme.accounting.v1.CancelAllocationWorkflowResponse;
import com.acme.accounting.v1.InspectAllocationWorkflowRequest;
import com.acme.accounting.v1.InspectAllocationWorkflowResponse;
import com.acme.accounting.v1.ListAllocationWorkflowsRequest;
import com.acme.accounting.v1.ListAllocationWorkflowsResponse;
import com.acme.accounting.v1.PauseAllocationWorkflowRequest;
import com.acme.accounting.v1.PauseAllocationWorkflowResponse;
import com.acme.accounting.v1.ResumeAllocationWorkflowRequest;
import com.acme.accounting.v1.ResumeAllocationWorkflowResponse;
import com.acme.accounting.v1.RetryAllocationWorkflowRequest;
import com.acme.accounting.v1.RetryAllocationWorkflowResponse;
import com.acme.accounting.v1.SignalAllocationWorkflowRequest;
import com.acme.accounting.v1.SignalAllocationWorkflowResponse;
import com.acme.accounting.v1.StartAllocationWorkflowRequest;
import com.acme.accounting.v1.StartAllocationWorkflowResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes an allocation from start to finish.
 */
@GrpcService
public class AllocationWorkflowEndpoint extends AllocationWorkflowServiceGrpc.AllocationWorkflowServiceImplBase {
    protected final AllocationWorkflowApplication application;

    public AllocationWorkflowEndpoint(AllocationWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts an allocation.
     */
    @Override
    public void startAllocationWorkflow(StartAllocationWorkflowRequest request, StreamObserver<StartAllocationWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startAllocationWorkflow(request));
    }

    /**
     * Advances an allocation.
     */
    @Override
    public void advanceAllocationWorkflow(AdvanceAllocationWorkflowRequest request, StreamObserver<AdvanceAllocationWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceAllocationWorkflow(request));
    }

    /**
     * Cancels an allocation.
     */
    @Override
    public void cancelAllocationWorkflow(CancelAllocationWorkflowRequest request, StreamObserver<CancelAllocationWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelAllocationWorkflow(request));
    }

    /**
     * Retrys an allocation.
     */
    @Override
    public void retryAllocationWorkflow(RetryAllocationWorkflowRequest request, StreamObserver<RetryAllocationWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryAllocationWorkflow(request));
    }

    /**
     * Pauses an allocation.
     */
    @Override
    public void pauseAllocationWorkflow(PauseAllocationWorkflowRequest request, StreamObserver<PauseAllocationWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseAllocationWorkflow(request));
    }

    /**
     * Resumes an allocation.
     */
    @Override
    public void resumeAllocationWorkflow(ResumeAllocationWorkflowRequest request, StreamObserver<ResumeAllocationWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeAllocationWorkflow(request));
    }

    /**
     * Returns an allocation.
     */
    @Override
    public void inspectAllocationWorkflow(InspectAllocationWorkflowRequest request, StreamObserver<InspectAllocationWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectAllocationWorkflow(request));
    }

    /**
     * Lists allocations a page at a time.
     */
    @Override
    public void listAllocationWorkflows(ListAllocationWorkflowsRequest request, StreamObserver<ListAllocationWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listAllocationWorkflows(request));
    }

    /**
     * Signals an allocation.
     */
    @Override
    public void signalAllocationWorkflow(SignalAllocationWorkflowRequest request, StreamObserver<SignalAllocationWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalAllocationWorkflow(request));
    }
}
