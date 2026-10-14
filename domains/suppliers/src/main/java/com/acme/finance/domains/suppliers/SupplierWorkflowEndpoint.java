package com.acme.finance.domains.suppliers;

import com.acme.finance.platform.RpcSupport;
import com.acme.suppliers.v1.AdvanceSupplierWorkflowRequest;
import com.acme.suppliers.v1.AdvanceSupplierWorkflowResponse;
import com.acme.suppliers.v1.CancelSupplierWorkflowRequest;
import com.acme.suppliers.v1.CancelSupplierWorkflowResponse;
import com.acme.suppliers.v1.InspectSupplierWorkflowRequest;
import com.acme.suppliers.v1.InspectSupplierWorkflowResponse;
import com.acme.suppliers.v1.ListSupplierWorkflowsRequest;
import com.acme.suppliers.v1.ListSupplierWorkflowsResponse;
import com.acme.suppliers.v1.PauseSupplierWorkflowRequest;
import com.acme.suppliers.v1.PauseSupplierWorkflowResponse;
import com.acme.suppliers.v1.ResumeSupplierWorkflowRequest;
import com.acme.suppliers.v1.ResumeSupplierWorkflowResponse;
import com.acme.suppliers.v1.RetrySupplierWorkflowRequest;
import com.acme.suppliers.v1.RetrySupplierWorkflowResponse;
import com.acme.suppliers.v1.SignalSupplierWorkflowRequest;
import com.acme.suppliers.v1.SignalSupplierWorkflowResponse;
import com.acme.suppliers.v1.StartSupplierWorkflowRequest;
import com.acme.suppliers.v1.StartSupplierWorkflowResponse;
import com.acme.suppliers.v1.SupplierWorkflowServiceGrpc;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes a supplier from start to finish.
 */
@GrpcService
public class SupplierWorkflowEndpoint extends SupplierWorkflowServiceGrpc.SupplierWorkflowServiceImplBase {
    protected final SupplierWorkflowApplication application;

    public SupplierWorkflowEndpoint(SupplierWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a supplier.
     */
    @Override
    public void startSupplierWorkflow(StartSupplierWorkflowRequest request, StreamObserver<StartSupplierWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startSupplierWorkflow(request));
    }

    /**
     * Advances a supplier.
     */
    @Override
    public void advanceSupplierWorkflow(AdvanceSupplierWorkflowRequest request, StreamObserver<AdvanceSupplierWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceSupplierWorkflow(request));
    }

    /**
     * Cancels a supplier.
     */
    @Override
    public void cancelSupplierWorkflow(CancelSupplierWorkflowRequest request, StreamObserver<CancelSupplierWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelSupplierWorkflow(request));
    }

    /**
     * Retrys a supplier.
     */
    @Override
    public void retrySupplierWorkflow(RetrySupplierWorkflowRequest request, StreamObserver<RetrySupplierWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retrySupplierWorkflow(request));
    }

    /**
     * Pauses a supplier.
     */
    @Override
    public void pauseSupplierWorkflow(PauseSupplierWorkflowRequest request, StreamObserver<PauseSupplierWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseSupplierWorkflow(request));
    }

    /**
     * Resumes a supplier.
     */
    @Override
    public void resumeSupplierWorkflow(ResumeSupplierWorkflowRequest request, StreamObserver<ResumeSupplierWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeSupplierWorkflow(request));
    }

    /**
     * Returns a supplier.
     */
    @Override
    public void inspectSupplierWorkflow(InspectSupplierWorkflowRequest request, StreamObserver<InspectSupplierWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectSupplierWorkflow(request));
    }

    /**
     * Lists suppliers a page at a time.
     */
    @Override
    public void listSupplierWorkflows(ListSupplierWorkflowsRequest request, StreamObserver<ListSupplierWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listSupplierWorkflows(request));
    }

    /**
     * Signals a supplier.
     */
    @Override
    public void signalSupplierWorkflow(SignalSupplierWorkflowRequest request, StreamObserver<SignalSupplierWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalSupplierWorkflow(request));
    }
}
