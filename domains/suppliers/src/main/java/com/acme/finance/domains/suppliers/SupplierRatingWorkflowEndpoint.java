package com.acme.finance.domains.suppliers;

import com.acme.finance.platform.RpcSupport;
import com.acme.suppliers.v1.AdvanceSupplierRatingWorkflowRequest;
import com.acme.suppliers.v1.AdvanceSupplierRatingWorkflowResponse;
import com.acme.suppliers.v1.CancelSupplierRatingWorkflowRequest;
import com.acme.suppliers.v1.CancelSupplierRatingWorkflowResponse;
import com.acme.suppliers.v1.InspectSupplierRatingWorkflowRequest;
import com.acme.suppliers.v1.InspectSupplierRatingWorkflowResponse;
import com.acme.suppliers.v1.ListSupplierRatingWorkflowsRequest;
import com.acme.suppliers.v1.ListSupplierRatingWorkflowsResponse;
import com.acme.suppliers.v1.ResumeSupplierRatingWorkflowRequest;
import com.acme.suppliers.v1.ResumeSupplierRatingWorkflowResponse;
import com.acme.suppliers.v1.RetrySupplierRatingWorkflowRequest;
import com.acme.suppliers.v1.RetrySupplierRatingWorkflowResponse;
import com.acme.suppliers.v1.SignalSupplierRatingWorkflowRequest;
import com.acme.suppliers.v1.SignalSupplierRatingWorkflowResponse;
import com.acme.suppliers.v1.StartSupplierRatingWorkflowRequest;
import com.acme.suppliers.v1.StartSupplierRatingWorkflowResponse;
import com.acme.suppliers.v1.SupplierRatingWorkflowServiceGrpc;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes a supplier rating from start to finish.
 */
@GrpcService
public class SupplierRatingWorkflowEndpoint extends SupplierRatingWorkflowServiceGrpc.SupplierRatingWorkflowServiceImplBase {
    protected final SupplierRatingWorkflowApplication application;

    public SupplierRatingWorkflowEndpoint(SupplierRatingWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a supplier rating.
     */
    @Override
    public void startSupplierRatingWorkflow(StartSupplierRatingWorkflowRequest request, StreamObserver<StartSupplierRatingWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startSupplierRatingWorkflow(request));
    }

    /**
     * Advances a supplier rating.
     */
    @Override
    public void advanceSupplierRatingWorkflow(AdvanceSupplierRatingWorkflowRequest request, StreamObserver<AdvanceSupplierRatingWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceSupplierRatingWorkflow(request));
    }

    /**
     * Cancels a supplier rating.
     */
    @Override
    public void cancelSupplierRatingWorkflow(CancelSupplierRatingWorkflowRequest request, StreamObserver<CancelSupplierRatingWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelSupplierRatingWorkflow(request));
    }

    /**
     * Retrys a supplier rating.
     */
    @Override
    public void retrySupplierRatingWorkflow(RetrySupplierRatingWorkflowRequest request, StreamObserver<RetrySupplierRatingWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retrySupplierRatingWorkflow(request));
    }

    /**
     * Resumes a supplier rating.
     */
    @Override
    public void resumeSupplierRatingWorkflow(ResumeSupplierRatingWorkflowRequest request, StreamObserver<ResumeSupplierRatingWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeSupplierRatingWorkflow(request));
    }

    /**
     * Returns a supplier rating.
     */
    @Override
    public void inspectSupplierRatingWorkflow(InspectSupplierRatingWorkflowRequest request, StreamObserver<InspectSupplierRatingWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectSupplierRatingWorkflow(request));
    }

    /**
     * Lists supplier ratings a page at a time.
     */
    @Override
    public void listSupplierRatingWorkflows(ListSupplierRatingWorkflowsRequest request, StreamObserver<ListSupplierRatingWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listSupplierRatingWorkflows(request));
    }

    /**
     * Signals a supplier rating.
     */
    @Override
    public void signalSupplierRatingWorkflow(SignalSupplierRatingWorkflowRequest request, StreamObserver<SignalSupplierRatingWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalSupplierRatingWorkflow(request));
    }
}
