package com.acme.finance.domains.treasury;

import com.acme.finance.platform.RpcSupport;
import com.acme.treasury.v1.AdvanceFxHedgeWorkflowRequest;
import com.acme.treasury.v1.AdvanceFxHedgeWorkflowResponse;
import com.acme.treasury.v1.CancelFxHedgeWorkflowRequest;
import com.acme.treasury.v1.CancelFxHedgeWorkflowResponse;
import com.acme.treasury.v1.FxHedgeWorkflowServiceGrpc;
import com.acme.treasury.v1.InspectFxHedgeWorkflowRequest;
import com.acme.treasury.v1.InspectFxHedgeWorkflowResponse;
import com.acme.treasury.v1.ListFxHedgeWorkflowsRequest;
import com.acme.treasury.v1.ListFxHedgeWorkflowsResponse;
import com.acme.treasury.v1.PauseFxHedgeWorkflowRequest;
import com.acme.treasury.v1.PauseFxHedgeWorkflowResponse;
import com.acme.treasury.v1.ResumeFxHedgeWorkflowRequest;
import com.acme.treasury.v1.ResumeFxHedgeWorkflowResponse;
import com.acme.treasury.v1.RetryFxHedgeWorkflowRequest;
import com.acme.treasury.v1.RetryFxHedgeWorkflowResponse;
import com.acme.treasury.v1.SignalFxHedgeWorkflowRequest;
import com.acme.treasury.v1.SignalFxHedgeWorkflowResponse;
import com.acme.treasury.v1.StartFxHedgeWorkflowRequest;
import com.acme.treasury.v1.StartFxHedgeWorkflowResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes a fx hedge from start to finish.
 */
@GrpcService
public class FxHedgeWorkflowEndpoint extends FxHedgeWorkflowServiceGrpc.FxHedgeWorkflowServiceImplBase {
    protected final FxHedgeWorkflowApplication application;

    public FxHedgeWorkflowEndpoint(FxHedgeWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a fx hedge.
     */
    @Override
    public void startFxHedgeWorkflow(StartFxHedgeWorkflowRequest request, StreamObserver<StartFxHedgeWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startFxHedgeWorkflow(request));
    }

    /**
     * Advances a fx hedge.
     */
    @Override
    public void advanceFxHedgeWorkflow(AdvanceFxHedgeWorkflowRequest request, StreamObserver<AdvanceFxHedgeWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceFxHedgeWorkflow(request));
    }

    /**
     * Cancels a fx hedge.
     */
    @Override
    public void cancelFxHedgeWorkflow(CancelFxHedgeWorkflowRequest request, StreamObserver<CancelFxHedgeWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelFxHedgeWorkflow(request));
    }

    /**
     * Retrys a fx hedge.
     */
    @Override
    public void retryFxHedgeWorkflow(RetryFxHedgeWorkflowRequest request, StreamObserver<RetryFxHedgeWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryFxHedgeWorkflow(request));
    }

    /**
     * Pauses a fx hedge.
     */
    @Override
    public void pauseFxHedgeWorkflow(PauseFxHedgeWorkflowRequest request, StreamObserver<PauseFxHedgeWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseFxHedgeWorkflow(request));
    }

    /**
     * Resumes a fx hedge.
     */
    @Override
    public void resumeFxHedgeWorkflow(ResumeFxHedgeWorkflowRequest request, StreamObserver<ResumeFxHedgeWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeFxHedgeWorkflow(request));
    }

    /**
     * Returns a fx hedge.
     */
    @Override
    public void inspectFxHedgeWorkflow(InspectFxHedgeWorkflowRequest request, StreamObserver<InspectFxHedgeWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectFxHedgeWorkflow(request));
    }

    /**
     * Lists fx hedges a page at a time.
     */
    @Override
    public void listFxHedgeWorkflows(ListFxHedgeWorkflowsRequest request, StreamObserver<ListFxHedgeWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listFxHedgeWorkflows(request));
    }

    /**
     * Signals a fx hedge.
     */
    @Override
    public void signalFxHedgeWorkflow(SignalFxHedgeWorkflowRequest request, StreamObserver<SignalFxHedgeWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalFxHedgeWorkflow(request));
    }
}
