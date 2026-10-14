package com.acme.finance.domains.procurement;

import com.acme.finance.platform.RpcSupport;
import com.acme.procurement.v1.AdvancePurchaseOrderWorkflowRequest;
import com.acme.procurement.v1.AdvancePurchaseOrderWorkflowResponse;
import com.acme.procurement.v1.CancelPurchaseOrderWorkflowRequest;
import com.acme.procurement.v1.CancelPurchaseOrderWorkflowResponse;
import com.acme.procurement.v1.InspectPurchaseOrderWorkflowRequest;
import com.acme.procurement.v1.InspectPurchaseOrderWorkflowResponse;
import com.acme.procurement.v1.ListPurchaseOrderWorkflowsRequest;
import com.acme.procurement.v1.ListPurchaseOrderWorkflowsResponse;
import com.acme.procurement.v1.PurchaseOrderWorkflowServiceGrpc;
import com.acme.procurement.v1.RetryPurchaseOrderWorkflowRequest;
import com.acme.procurement.v1.RetryPurchaseOrderWorkflowResponse;
import com.acme.procurement.v1.SignalPurchaseOrderWorkflowRequest;
import com.acme.procurement.v1.SignalPurchaseOrderWorkflowResponse;
import com.acme.procurement.v1.StartPurchaseOrderWorkflowRequest;
import com.acme.procurement.v1.StartPurchaseOrderWorkflowResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes a purchase order from start to finish.
 */
@GrpcService
public class PurchaseOrderWorkflowEndpoint extends PurchaseOrderWorkflowServiceGrpc.PurchaseOrderWorkflowServiceImplBase {
    protected final PurchaseOrderWorkflowApplication application;

    public PurchaseOrderWorkflowEndpoint(PurchaseOrderWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a purchase order.
     */
    @Override
    public void startPurchaseOrderWorkflow(StartPurchaseOrderWorkflowRequest request, StreamObserver<StartPurchaseOrderWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startPurchaseOrderWorkflow(request));
    }

    /**
     * Advances a purchase order.
     */
    @Override
    public void advancePurchaseOrderWorkflow(AdvancePurchaseOrderWorkflowRequest request, StreamObserver<AdvancePurchaseOrderWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advancePurchaseOrderWorkflow(request));
    }

    /**
     * Cancels a purchase order.
     */
    @Override
    public void cancelPurchaseOrderWorkflow(CancelPurchaseOrderWorkflowRequest request, StreamObserver<CancelPurchaseOrderWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelPurchaseOrderWorkflow(request));
    }

    /**
     * Retrys a purchase order.
     */
    @Override
    public void retryPurchaseOrderWorkflow(RetryPurchaseOrderWorkflowRequest request, StreamObserver<RetryPurchaseOrderWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryPurchaseOrderWorkflow(request));
    }

    /**
     * Returns a purchase order.
     */
    @Override
    public void inspectPurchaseOrderWorkflow(InspectPurchaseOrderWorkflowRequest request, StreamObserver<InspectPurchaseOrderWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectPurchaseOrderWorkflow(request));
    }

    /**
     * Lists purchase orders a page at a time.
     */
    @Override
    public void listPurchaseOrderWorkflows(ListPurchaseOrderWorkflowsRequest request, StreamObserver<ListPurchaseOrderWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPurchaseOrderWorkflows(request));
    }

    /**
     * Signals a purchase order.
     */
    @Override
    public void signalPurchaseOrderWorkflow(SignalPurchaseOrderWorkflowRequest request, StreamObserver<SignalPurchaseOrderWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalPurchaseOrderWorkflow(request));
    }
}
