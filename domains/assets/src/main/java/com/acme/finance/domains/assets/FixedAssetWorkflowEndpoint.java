package com.acme.finance.domains.assets;

import com.acme.assets.v1.AdvanceFixedAssetWorkflowRequest;
import com.acme.assets.v1.AdvanceFixedAssetWorkflowResponse;
import com.acme.assets.v1.FixedAssetWorkflowServiceGrpc;
import com.acme.assets.v1.InspectFixedAssetWorkflowRequest;
import com.acme.assets.v1.InspectFixedAssetWorkflowResponse;
import com.acme.assets.v1.ListFixedAssetWorkflowsRequest;
import com.acme.assets.v1.ListFixedAssetWorkflowsResponse;
import com.acme.assets.v1.ResumeFixedAssetWorkflowRequest;
import com.acme.assets.v1.ResumeFixedAssetWorkflowResponse;
import com.acme.assets.v1.RetryFixedAssetWorkflowRequest;
import com.acme.assets.v1.RetryFixedAssetWorkflowResponse;
import com.acme.assets.v1.SignalFixedAssetWorkflowRequest;
import com.acme.assets.v1.SignalFixedAssetWorkflowResponse;
import com.acme.assets.v1.StartFixedAssetWorkflowRequest;
import com.acme.assets.v1.StartFixedAssetWorkflowResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes a fixed asset from start to finish.
 */
@GrpcService
public class FixedAssetWorkflowEndpoint extends FixedAssetWorkflowServiceGrpc.FixedAssetWorkflowServiceImplBase {
    protected final FixedAssetWorkflowApplication application;

    public FixedAssetWorkflowEndpoint(FixedAssetWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a fixed asset.
     */
    @Override
    public void startFixedAssetWorkflow(StartFixedAssetWorkflowRequest request, StreamObserver<StartFixedAssetWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startFixedAssetWorkflow(request));
    }

    /**
     * Advances a fixed asset.
     */
    @Override
    public void advanceFixedAssetWorkflow(AdvanceFixedAssetWorkflowRequest request, StreamObserver<AdvanceFixedAssetWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceFixedAssetWorkflow(request));
    }

    /**
     * Retrys a fixed asset.
     */
    @Override
    public void retryFixedAssetWorkflow(RetryFixedAssetWorkflowRequest request, StreamObserver<RetryFixedAssetWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryFixedAssetWorkflow(request));
    }

    /**
     * Resumes a fixed asset.
     */
    @Override
    public void resumeFixedAssetWorkflow(ResumeFixedAssetWorkflowRequest request, StreamObserver<ResumeFixedAssetWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeFixedAssetWorkflow(request));
    }

    /**
     * Returns a fixed asset.
     */
    @Override
    public void inspectFixedAssetWorkflow(InspectFixedAssetWorkflowRequest request, StreamObserver<InspectFixedAssetWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectFixedAssetWorkflow(request));
    }

    /**
     * Lists fixed assets a page at a time.
     */
    @Override
    public void listFixedAssetWorkflows(ListFixedAssetWorkflowsRequest request, StreamObserver<ListFixedAssetWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listFixedAssetWorkflows(request));
    }

    /**
     * Signals a fixed asset.
     */
    @Override
    public void signalFixedAssetWorkflow(SignalFixedAssetWorkflowRequest request, StreamObserver<SignalFixedAssetWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalFixedAssetWorkflow(request));
    }
}
