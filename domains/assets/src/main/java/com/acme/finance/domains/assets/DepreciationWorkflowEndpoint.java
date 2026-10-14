package com.acme.finance.domains.assets;

import com.acme.assets.v1.AdvanceDepreciationWorkflowRequest;
import com.acme.assets.v1.AdvanceDepreciationWorkflowResponse;
import com.acme.assets.v1.CancelDepreciationWorkflowRequest;
import com.acme.assets.v1.CancelDepreciationWorkflowResponse;
import com.acme.assets.v1.DepreciationWorkflowServiceGrpc;
import com.acme.assets.v1.InspectDepreciationWorkflowRequest;
import com.acme.assets.v1.InspectDepreciationWorkflowResponse;
import com.acme.assets.v1.ListDepreciationWorkflowsRequest;
import com.acme.assets.v1.ListDepreciationWorkflowsResponse;
import com.acme.assets.v1.PauseDepreciationWorkflowRequest;
import com.acme.assets.v1.PauseDepreciationWorkflowResponse;
import com.acme.assets.v1.ResumeDepreciationWorkflowRequest;
import com.acme.assets.v1.ResumeDepreciationWorkflowResponse;
import com.acme.assets.v1.RetryDepreciationWorkflowRequest;
import com.acme.assets.v1.RetryDepreciationWorkflowResponse;
import com.acme.assets.v1.StartDepreciationWorkflowRequest;
import com.acme.assets.v1.StartDepreciationWorkflowResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes a depreciation from start to finish.
 */
@GrpcService
public class DepreciationWorkflowEndpoint extends DepreciationWorkflowServiceGrpc.DepreciationWorkflowServiceImplBase {
    protected final DepreciationWorkflowApplication application;

    public DepreciationWorkflowEndpoint(DepreciationWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a depreciation.
     */
    @Override
    public void startDepreciationWorkflow(StartDepreciationWorkflowRequest request, StreamObserver<StartDepreciationWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startDepreciationWorkflow(request));
    }

    /**
     * Advances a depreciation.
     */
    @Override
    public void advanceDepreciationWorkflow(AdvanceDepreciationWorkflowRequest request, StreamObserver<AdvanceDepreciationWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceDepreciationWorkflow(request));
    }

    /**
     * Cancels a depreciation.
     */
    @Override
    public void cancelDepreciationWorkflow(CancelDepreciationWorkflowRequest request, StreamObserver<CancelDepreciationWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelDepreciationWorkflow(request));
    }

    /**
     * Retrys a depreciation.
     */
    @Override
    public void retryDepreciationWorkflow(RetryDepreciationWorkflowRequest request, StreamObserver<RetryDepreciationWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryDepreciationWorkflow(request));
    }

    /**
     * Pauses a depreciation.
     */
    @Override
    public void pauseDepreciationWorkflow(PauseDepreciationWorkflowRequest request, StreamObserver<PauseDepreciationWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseDepreciationWorkflow(request));
    }

    /**
     * Resumes a depreciation.
     */
    @Override
    public void resumeDepreciationWorkflow(ResumeDepreciationWorkflowRequest request, StreamObserver<ResumeDepreciationWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeDepreciationWorkflow(request));
    }

    /**
     * Returns a depreciation.
     */
    @Override
    public void inspectDepreciationWorkflow(InspectDepreciationWorkflowRequest request, StreamObserver<InspectDepreciationWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectDepreciationWorkflow(request));
    }

    /**
     * Lists depreciations a page at a time.
     */
    @Override
    public void listDepreciationWorkflows(ListDepreciationWorkflowsRequest request, StreamObserver<ListDepreciationWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listDepreciationWorkflows(request));
    }
}
