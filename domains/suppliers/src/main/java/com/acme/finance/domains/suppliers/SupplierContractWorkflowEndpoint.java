package com.acme.finance.domains.suppliers;

import com.acme.finance.platform.RpcSupport;
import com.acme.suppliers.v1.AdvanceSupplierContractWorkflowRequest;
import com.acme.suppliers.v1.AdvanceSupplierContractWorkflowResponse;
import com.acme.suppliers.v1.CancelSupplierContractWorkflowRequest;
import com.acme.suppliers.v1.CancelSupplierContractWorkflowResponse;
import com.acme.suppliers.v1.InspectSupplierContractWorkflowRequest;
import com.acme.suppliers.v1.InspectSupplierContractWorkflowResponse;
import com.acme.suppliers.v1.ListSupplierContractWorkflowsRequest;
import com.acme.suppliers.v1.ListSupplierContractWorkflowsResponse;
import com.acme.suppliers.v1.PauseSupplierContractWorkflowRequest;
import com.acme.suppliers.v1.PauseSupplierContractWorkflowResponse;
import com.acme.suppliers.v1.ResumeSupplierContractWorkflowRequest;
import com.acme.suppliers.v1.ResumeSupplierContractWorkflowResponse;
import com.acme.suppliers.v1.RetrySupplierContractWorkflowRequest;
import com.acme.suppliers.v1.RetrySupplierContractWorkflowResponse;
import com.acme.suppliers.v1.SignalSupplierContractWorkflowRequest;
import com.acme.suppliers.v1.SignalSupplierContractWorkflowResponse;
import com.acme.suppliers.v1.StartSupplierContractWorkflowRequest;
import com.acme.suppliers.v1.StartSupplierContractWorkflowResponse;
import com.acme.suppliers.v1.SupplierContractWorkflowServiceGrpc;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes a supplier contract from start to finish.
 */
@GrpcService
public class SupplierContractWorkflowEndpoint extends SupplierContractWorkflowServiceGrpc.SupplierContractWorkflowServiceImplBase {
    protected final SupplierContractWorkflowApplication application;

    public SupplierContractWorkflowEndpoint(SupplierContractWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a supplier contract.
     */
    @Override
    public void startSupplierContractWorkflow(StartSupplierContractWorkflowRequest request, StreamObserver<StartSupplierContractWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startSupplierContractWorkflow(request));
    }

    /**
     * Advances a supplier contract.
     */
    @Override
    public void advanceSupplierContractWorkflow(AdvanceSupplierContractWorkflowRequest request, StreamObserver<AdvanceSupplierContractWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceSupplierContractWorkflow(request));
    }

    /**
     * Cancels a supplier contract.
     */
    @Override
    public void cancelSupplierContractWorkflow(CancelSupplierContractWorkflowRequest request, StreamObserver<CancelSupplierContractWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelSupplierContractWorkflow(request));
    }

    /**
     * Retrys a supplier contract.
     */
    @Override
    public void retrySupplierContractWorkflow(RetrySupplierContractWorkflowRequest request, StreamObserver<RetrySupplierContractWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retrySupplierContractWorkflow(request));
    }

    /**
     * Pauses a supplier contract.
     */
    @Override
    public void pauseSupplierContractWorkflow(PauseSupplierContractWorkflowRequest request, StreamObserver<PauseSupplierContractWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseSupplierContractWorkflow(request));
    }

    /**
     * Resumes a supplier contract.
     */
    @Override
    public void resumeSupplierContractWorkflow(ResumeSupplierContractWorkflowRequest request, StreamObserver<ResumeSupplierContractWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeSupplierContractWorkflow(request));
    }

    /**
     * Returns a supplier contract.
     */
    @Override
    public void inspectSupplierContractWorkflow(InspectSupplierContractWorkflowRequest request, StreamObserver<InspectSupplierContractWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectSupplierContractWorkflow(request));
    }

    /**
     * Lists supplier contracts a page at a time.
     */
    @Override
    public void listSupplierContractWorkflows(ListSupplierContractWorkflowsRequest request, StreamObserver<ListSupplierContractWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listSupplierContractWorkflows(request));
    }

    /**
     * Signals a supplier contract.
     */
    @Override
    public void signalSupplierContractWorkflow(SignalSupplierContractWorkflowRequest request, StreamObserver<SignalSupplierContractWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalSupplierContractWorkflow(request));
    }
}
