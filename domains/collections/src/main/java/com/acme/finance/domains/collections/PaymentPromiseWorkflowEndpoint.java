package com.acme.finance.domains.collections;

import com.acme.collections.v1.AdvancePaymentPromiseWorkflowRequest;
import com.acme.collections.v1.AdvancePaymentPromiseWorkflowResponse;
import com.acme.collections.v1.CancelPaymentPromiseWorkflowRequest;
import com.acme.collections.v1.CancelPaymentPromiseWorkflowResponse;
import com.acme.collections.v1.InspectPaymentPromiseWorkflowRequest;
import com.acme.collections.v1.InspectPaymentPromiseWorkflowResponse;
import com.acme.collections.v1.ListPaymentPromiseWorkflowsRequest;
import com.acme.collections.v1.ListPaymentPromiseWorkflowsResponse;
import com.acme.collections.v1.PaymentPromiseWorkflowServiceGrpc;
import com.acme.collections.v1.ResumePaymentPromiseWorkflowRequest;
import com.acme.collections.v1.ResumePaymentPromiseWorkflowResponse;
import com.acme.collections.v1.RetryPaymentPromiseWorkflowRequest;
import com.acme.collections.v1.RetryPaymentPromiseWorkflowResponse;
import com.acme.collections.v1.SignalPaymentPromiseWorkflowRequest;
import com.acme.collections.v1.SignalPaymentPromiseWorkflowResponse;
import com.acme.collections.v1.StartPaymentPromiseWorkflowRequest;
import com.acme.collections.v1.StartPaymentPromiseWorkflowResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes a payment promise from start to finish.
 */
@GrpcService
public class PaymentPromiseWorkflowEndpoint extends PaymentPromiseWorkflowServiceGrpc.PaymentPromiseWorkflowServiceImplBase {
    protected final PaymentPromiseWorkflowApplication application;

    public PaymentPromiseWorkflowEndpoint(PaymentPromiseWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a payment promise.
     */
    @Override
    public void startPaymentPromiseWorkflow(StartPaymentPromiseWorkflowRequest request, StreamObserver<StartPaymentPromiseWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startPaymentPromiseWorkflow(request));
    }

    /**
     * Advances a payment promise.
     */
    @Override
    public void advancePaymentPromiseWorkflow(AdvancePaymentPromiseWorkflowRequest request, StreamObserver<AdvancePaymentPromiseWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advancePaymentPromiseWorkflow(request));
    }

    /**
     * Cancels a payment promise.
     */
    @Override
    public void cancelPaymentPromiseWorkflow(CancelPaymentPromiseWorkflowRequest request, StreamObserver<CancelPaymentPromiseWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelPaymentPromiseWorkflow(request));
    }

    /**
     * Retrys a payment promise.
     */
    @Override
    public void retryPaymentPromiseWorkflow(RetryPaymentPromiseWorkflowRequest request, StreamObserver<RetryPaymentPromiseWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryPaymentPromiseWorkflow(request));
    }

    /**
     * Resumes a payment promise.
     */
    @Override
    public void resumePaymentPromiseWorkflow(ResumePaymentPromiseWorkflowRequest request, StreamObserver<ResumePaymentPromiseWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumePaymentPromiseWorkflow(request));
    }

    /**
     * Returns a payment promise.
     */
    @Override
    public void inspectPaymentPromiseWorkflow(InspectPaymentPromiseWorkflowRequest request, StreamObserver<InspectPaymentPromiseWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectPaymentPromiseWorkflow(request));
    }

    /**
     * Lists payment promises a page at a time.
     */
    @Override
    public void listPaymentPromiseWorkflows(ListPaymentPromiseWorkflowsRequest request, StreamObserver<ListPaymentPromiseWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPaymentPromiseWorkflows(request));
    }

    /**
     * Signals a payment promise.
     */
    @Override
    public void signalPaymentPromiseWorkflow(SignalPaymentPromiseWorkflowRequest request, StreamObserver<SignalPaymentPromiseWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalPaymentPromiseWorkflow(request));
    }
}
