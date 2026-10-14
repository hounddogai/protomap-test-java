package com.acme.finance.domains.procurement;

import com.acme.finance.platform.RpcSupport;
import com.acme.procurement.v1.AdvancePurchaseRequisitionWorkflowRequest;
import com.acme.procurement.v1.AdvancePurchaseRequisitionWorkflowResponse;
import com.acme.procurement.v1.CancelPurchaseRequisitionWorkflowRequest;
import com.acme.procurement.v1.CancelPurchaseRequisitionWorkflowResponse;
import com.acme.procurement.v1.InspectPurchaseRequisitionWorkflowRequest;
import com.acme.procurement.v1.InspectPurchaseRequisitionWorkflowResponse;
import com.acme.procurement.v1.ListPurchaseRequisitionWorkflowsRequest;
import com.acme.procurement.v1.ListPurchaseRequisitionWorkflowsResponse;
import com.acme.procurement.v1.PurchaseRequisitionWorkflowServiceGrpc;
import com.acme.procurement.v1.ResumePurchaseRequisitionWorkflowRequest;
import com.acme.procurement.v1.ResumePurchaseRequisitionWorkflowResponse;
import com.acme.procurement.v1.RetryPurchaseRequisitionWorkflowRequest;
import com.acme.procurement.v1.RetryPurchaseRequisitionWorkflowResponse;
import com.acme.procurement.v1.SignalPurchaseRequisitionWorkflowRequest;
import com.acme.procurement.v1.SignalPurchaseRequisitionWorkflowResponse;
import com.acme.procurement.v1.StartPurchaseRequisitionWorkflowRequest;
import com.acme.procurement.v1.StartPurchaseRequisitionWorkflowResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes a purchase requisition from start to finish.
 */
@GrpcService
public class PurchaseRequisitionWorkflowEndpoint extends PurchaseRequisitionWorkflowServiceGrpc.PurchaseRequisitionWorkflowServiceImplBase {
    protected final PurchaseRequisitionWorkflowApplication application;

    public PurchaseRequisitionWorkflowEndpoint(PurchaseRequisitionWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a purchase requisition.
     */
    @Override
    public void startPurchaseRequisitionWorkflow(StartPurchaseRequisitionWorkflowRequest request, StreamObserver<StartPurchaseRequisitionWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startPurchaseRequisitionWorkflow(request));
    }

    /**
     * Advances a purchase requisition.
     */
    @Override
    public void advancePurchaseRequisitionWorkflow(AdvancePurchaseRequisitionWorkflowRequest request, StreamObserver<AdvancePurchaseRequisitionWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advancePurchaseRequisitionWorkflow(request));
    }

    /**
     * Cancels a purchase requisition.
     */
    @Override
    public void cancelPurchaseRequisitionWorkflow(CancelPurchaseRequisitionWorkflowRequest request, StreamObserver<CancelPurchaseRequisitionWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelPurchaseRequisitionWorkflow(request));
    }

    /**
     * Retrys a purchase requisition.
     */
    @Override
    public void retryPurchaseRequisitionWorkflow(RetryPurchaseRequisitionWorkflowRequest request, StreamObserver<RetryPurchaseRequisitionWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryPurchaseRequisitionWorkflow(request));
    }

    /**
     * Resumes a purchase requisition.
     */
    @Override
    public void resumePurchaseRequisitionWorkflow(ResumePurchaseRequisitionWorkflowRequest request, StreamObserver<ResumePurchaseRequisitionWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumePurchaseRequisitionWorkflow(request));
    }

    /**
     * Returns a purchase requisition.
     */
    @Override
    public void inspectPurchaseRequisitionWorkflow(InspectPurchaseRequisitionWorkflowRequest request, StreamObserver<InspectPurchaseRequisitionWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectPurchaseRequisitionWorkflow(request));
    }

    /**
     * Lists purchase requisitions a page at a time.
     */
    @Override
    public void listPurchaseRequisitionWorkflows(ListPurchaseRequisitionWorkflowsRequest request, StreamObserver<ListPurchaseRequisitionWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPurchaseRequisitionWorkflows(request));
    }

    /**
     * Signals a purchase requisition.
     */
    @Override
    public void signalPurchaseRequisitionWorkflow(SignalPurchaseRequisitionWorkflowRequest request, StreamObserver<SignalPurchaseRequisitionWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalPurchaseRequisitionWorkflow(request));
    }
}
