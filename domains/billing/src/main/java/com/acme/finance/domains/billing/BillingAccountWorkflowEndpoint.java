package com.acme.finance.domains.billing;

import com.acme.billing.v1.AdvanceBillingAccountWorkflowRequest;
import com.acme.billing.v1.AdvanceBillingAccountWorkflowResponse;
import com.acme.billing.v1.BillingAccountWorkflowServiceGrpc;
import com.acme.billing.v1.CancelBillingAccountWorkflowRequest;
import com.acme.billing.v1.CancelBillingAccountWorkflowResponse;
import com.acme.billing.v1.InspectBillingAccountWorkflowRequest;
import com.acme.billing.v1.InspectBillingAccountWorkflowResponse;
import com.acme.billing.v1.ListBillingAccountWorkflowsRequest;
import com.acme.billing.v1.ListBillingAccountWorkflowsResponse;
import com.acme.billing.v1.PauseBillingAccountWorkflowRequest;
import com.acme.billing.v1.PauseBillingAccountWorkflowResponse;
import com.acme.billing.v1.ResumeBillingAccountWorkflowRequest;
import com.acme.billing.v1.ResumeBillingAccountWorkflowResponse;
import com.acme.billing.v1.RetryBillingAccountWorkflowRequest;
import com.acme.billing.v1.RetryBillingAccountWorkflowResponse;
import com.acme.billing.v1.SignalBillingAccountWorkflowRequest;
import com.acme.billing.v1.SignalBillingAccountWorkflowResponse;
import com.acme.billing.v1.StartBillingAccountWorkflowRequest;
import com.acme.billing.v1.StartBillingAccountWorkflowResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes a billing account from start to finish.
 */
@GrpcService
public class BillingAccountWorkflowEndpoint extends BillingAccountWorkflowServiceGrpc.BillingAccountWorkflowServiceImplBase {
    protected final BillingAccountWorkflowApplication application;

    public BillingAccountWorkflowEndpoint(BillingAccountWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a billing account.
     */
    @Override
    public void startBillingAccountWorkflow(StartBillingAccountWorkflowRequest request, StreamObserver<StartBillingAccountWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startBillingAccountWorkflow(request));
    }

    /**
     * Advances a billing account.
     */
    @Override
    public void advanceBillingAccountWorkflow(AdvanceBillingAccountWorkflowRequest request, StreamObserver<AdvanceBillingAccountWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceBillingAccountWorkflow(request));
    }

    /**
     * Cancels a billing account.
     */
    @Override
    public void cancelBillingAccountWorkflow(CancelBillingAccountWorkflowRequest request, StreamObserver<CancelBillingAccountWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelBillingAccountWorkflow(request));
    }

    /**
     * Retrys a billing account.
     */
    @Override
    public void retryBillingAccountWorkflow(RetryBillingAccountWorkflowRequest request, StreamObserver<RetryBillingAccountWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryBillingAccountWorkflow(request));
    }

    /**
     * Pauses a billing account.
     */
    @Override
    public void pauseBillingAccountWorkflow(PauseBillingAccountWorkflowRequest request, StreamObserver<PauseBillingAccountWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseBillingAccountWorkflow(request));
    }

    /**
     * Resumes a billing account.
     */
    @Override
    public void resumeBillingAccountWorkflow(ResumeBillingAccountWorkflowRequest request, StreamObserver<ResumeBillingAccountWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeBillingAccountWorkflow(request));
    }

    /**
     * Returns a billing account.
     */
    @Override
    public void inspectBillingAccountWorkflow(InspectBillingAccountWorkflowRequest request, StreamObserver<InspectBillingAccountWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectBillingAccountWorkflow(request));
    }

    /**
     * Lists billing accounts a page at a time.
     */
    @Override
    public void listBillingAccountWorkflows(ListBillingAccountWorkflowsRequest request, StreamObserver<ListBillingAccountWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listBillingAccountWorkflows(request));
    }

    /**
     * Signals a billing account.
     */
    @Override
    public void signalBillingAccountWorkflow(SignalBillingAccountWorkflowRequest request, StreamObserver<SignalBillingAccountWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalBillingAccountWorkflow(request));
    }
}
