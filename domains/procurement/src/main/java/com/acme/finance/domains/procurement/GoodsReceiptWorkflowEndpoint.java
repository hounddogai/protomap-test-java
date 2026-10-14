package com.acme.finance.domains.procurement;

import com.acme.finance.platform.RpcSupport;
import com.acme.procurement.v1.AdvanceGoodsReceiptWorkflowRequest;
import com.acme.procurement.v1.AdvanceGoodsReceiptWorkflowResponse;
import com.acme.procurement.v1.CancelGoodsReceiptWorkflowRequest;
import com.acme.procurement.v1.CancelGoodsReceiptWorkflowResponse;
import com.acme.procurement.v1.GoodsReceiptWorkflowServiceGrpc;
import com.acme.procurement.v1.ListGoodsReceiptWorkflowsRequest;
import com.acme.procurement.v1.ListGoodsReceiptWorkflowsResponse;
import com.acme.procurement.v1.PauseGoodsReceiptWorkflowRequest;
import com.acme.procurement.v1.PauseGoodsReceiptWorkflowResponse;
import com.acme.procurement.v1.ResumeGoodsReceiptWorkflowRequest;
import com.acme.procurement.v1.ResumeGoodsReceiptWorkflowResponse;
import com.acme.procurement.v1.SignalGoodsReceiptWorkflowRequest;
import com.acme.procurement.v1.SignalGoodsReceiptWorkflowResponse;
import com.acme.procurement.v1.StartGoodsReceiptWorkflowRequest;
import com.acme.procurement.v1.StartGoodsReceiptWorkflowResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes a goods receipt from start to finish.
 */
@GrpcService
public class GoodsReceiptWorkflowEndpoint extends GoodsReceiptWorkflowServiceGrpc.GoodsReceiptWorkflowServiceImplBase {
    protected final GoodsReceiptWorkflowApplication application;

    public GoodsReceiptWorkflowEndpoint(GoodsReceiptWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a goods receipt.
     */
    @Override
    public void startGoodsReceiptWorkflow(StartGoodsReceiptWorkflowRequest request, StreamObserver<StartGoodsReceiptWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startGoodsReceiptWorkflow(request));
    }

    /**
     * Advances a goods receipt.
     */
    @Override
    public void advanceGoodsReceiptWorkflow(AdvanceGoodsReceiptWorkflowRequest request, StreamObserver<AdvanceGoodsReceiptWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceGoodsReceiptWorkflow(request));
    }

    /**
     * Cancels a goods receipt.
     */
    @Override
    public void cancelGoodsReceiptWorkflow(CancelGoodsReceiptWorkflowRequest request, StreamObserver<CancelGoodsReceiptWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelGoodsReceiptWorkflow(request));
    }

    /**
     * Pauses a goods receipt.
     */
    @Override
    public void pauseGoodsReceiptWorkflow(PauseGoodsReceiptWorkflowRequest request, StreamObserver<PauseGoodsReceiptWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseGoodsReceiptWorkflow(request));
    }

    /**
     * Resumes a goods receipt.
     */
    @Override
    public void resumeGoodsReceiptWorkflow(ResumeGoodsReceiptWorkflowRequest request, StreamObserver<ResumeGoodsReceiptWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeGoodsReceiptWorkflow(request));
    }

    /**
     * Lists goods receipts a page at a time.
     */
    @Override
    public void listGoodsReceiptWorkflows(ListGoodsReceiptWorkflowsRequest request, StreamObserver<ListGoodsReceiptWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listGoodsReceiptWorkflows(request));
    }

    /**
     * Signals a goods receipt.
     */
    @Override
    public void signalGoodsReceiptWorkflow(SignalGoodsReceiptWorkflowRequest request, StreamObserver<SignalGoodsReceiptWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalGoodsReceiptWorkflow(request));
    }
}
