package com.acme.finance.domains.assets;

import com.acme.assets.v1.AdvanceAssetTransferWorkflowRequest;
import com.acme.assets.v1.AdvanceAssetTransferWorkflowResponse;
import com.acme.assets.v1.AssetTransferWorkflowServiceGrpc;
import com.acme.assets.v1.CancelAssetTransferWorkflowRequest;
import com.acme.assets.v1.CancelAssetTransferWorkflowResponse;
import com.acme.assets.v1.InspectAssetTransferWorkflowRequest;
import com.acme.assets.v1.InspectAssetTransferWorkflowResponse;
import com.acme.assets.v1.ListAssetTransferWorkflowsRequest;
import com.acme.assets.v1.ListAssetTransferWorkflowsResponse;
import com.acme.assets.v1.PauseAssetTransferWorkflowRequest;
import com.acme.assets.v1.PauseAssetTransferWorkflowResponse;
import com.acme.assets.v1.ResumeAssetTransferWorkflowRequest;
import com.acme.assets.v1.ResumeAssetTransferWorkflowResponse;
import com.acme.assets.v1.SignalAssetTransferWorkflowRequest;
import com.acme.assets.v1.SignalAssetTransferWorkflowResponse;
import com.acme.assets.v1.StartAssetTransferWorkflowRequest;
import com.acme.assets.v1.StartAssetTransferWorkflowResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Runs the long-lived workflow that takes an asset transfer from start to finish.
 */
public abstract class AbstractAssetTransferWorkflowEndpoint extends AssetTransferWorkflowServiceGrpc.AssetTransferWorkflowServiceImplBase {
    protected final AssetTransferWorkflowApplication application;

    protected AbstractAssetTransferWorkflowEndpoint(AssetTransferWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts an asset transfer.
     */
    @Override
    public void startAssetTransferWorkflow(StartAssetTransferWorkflowRequest request, StreamObserver<StartAssetTransferWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startAssetTransferWorkflow(request));
    }

    /**
     * Advances an asset transfer.
     */
    @Override
    public void advanceAssetTransferWorkflow(AdvanceAssetTransferWorkflowRequest request, StreamObserver<AdvanceAssetTransferWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceAssetTransferWorkflow(request));
    }

    /**
     * Cancels an asset transfer.
     */
    @Override
    public void cancelAssetTransferWorkflow(CancelAssetTransferWorkflowRequest request, StreamObserver<CancelAssetTransferWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelAssetTransferWorkflow(request));
    }

    /**
     * Pauses an asset transfer.
     */
    @Override
    public void pauseAssetTransferWorkflow(PauseAssetTransferWorkflowRequest request, StreamObserver<PauseAssetTransferWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseAssetTransferWorkflow(request));
    }

    /**
     * Resumes an asset transfer.
     */
    @Override
    public void resumeAssetTransferWorkflow(ResumeAssetTransferWorkflowRequest request, StreamObserver<ResumeAssetTransferWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeAssetTransferWorkflow(request));
    }

    /**
     * Returns an asset transfer.
     */
    @Override
    public void inspectAssetTransferWorkflow(InspectAssetTransferWorkflowRequest request, StreamObserver<InspectAssetTransferWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectAssetTransferWorkflow(request));
    }

    /**
     * Lists asset transfers a page at a time.
     */
    @Override
    public void listAssetTransferWorkflows(ListAssetTransferWorkflowsRequest request, StreamObserver<ListAssetTransferWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listAssetTransferWorkflows(request));
    }

    /**
     * Signals an asset transfer.
     */
    @Override
    public void signalAssetTransferWorkflow(SignalAssetTransferWorkflowRequest request, StreamObserver<SignalAssetTransferWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalAssetTransferWorkflow(request));
    }
}
