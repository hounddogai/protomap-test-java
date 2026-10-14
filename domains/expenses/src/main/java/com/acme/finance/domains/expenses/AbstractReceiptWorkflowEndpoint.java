package com.acme.finance.domains.expenses;

import com.acme.expenses.v1.AdvanceReceiptWorkflowRequest;
import com.acme.expenses.v1.AdvanceReceiptWorkflowResponse;
import com.acme.expenses.v1.InspectReceiptWorkflowRequest;
import com.acme.expenses.v1.InspectReceiptWorkflowResponse;
import com.acme.expenses.v1.PauseReceiptWorkflowRequest;
import com.acme.expenses.v1.PauseReceiptWorkflowResponse;
import com.acme.expenses.v1.ReceiptWorkflowServiceGrpc;
import com.acme.expenses.v1.RetryReceiptWorkflowRequest;
import com.acme.expenses.v1.RetryReceiptWorkflowResponse;
import com.acme.expenses.v1.SignalReceiptWorkflowRequest;
import com.acme.expenses.v1.SignalReceiptWorkflowResponse;
import com.acme.expenses.v1.StartReceiptWorkflowRequest;
import com.acme.expenses.v1.StartReceiptWorkflowResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Runs the long-lived workflow that takes a receipt from start to finish.
 */
public abstract class AbstractReceiptWorkflowEndpoint extends ReceiptWorkflowServiceGrpc.ReceiptWorkflowServiceImplBase {
    protected final ReceiptWorkflowApplication application;

    protected AbstractReceiptWorkflowEndpoint(ReceiptWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a receipt.
     */
    @Override
    public void startReceiptWorkflow(StartReceiptWorkflowRequest request, StreamObserver<StartReceiptWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startReceiptWorkflow(request));
    }

    /**
     * Advances a receipt.
     */
    @Override
    public void advanceReceiptWorkflow(AdvanceReceiptWorkflowRequest request, StreamObserver<AdvanceReceiptWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceReceiptWorkflow(request));
    }

    /**
     * Retrys a receipt.
     */
    @Override
    public void retryReceiptWorkflow(RetryReceiptWorkflowRequest request, StreamObserver<RetryReceiptWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryReceiptWorkflow(request));
    }

    /**
     * Pauses a receipt.
     */
    @Override
    public void pauseReceiptWorkflow(PauseReceiptWorkflowRequest request, StreamObserver<PauseReceiptWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseReceiptWorkflow(request));
    }

    /**
     * Returns a receipt.
     */
    @Override
    public void inspectReceiptWorkflow(InspectReceiptWorkflowRequest request, StreamObserver<InspectReceiptWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectReceiptWorkflow(request));
    }

    /**
     * Signals a receipt.
     */
    @Override
    public void signalReceiptWorkflow(SignalReceiptWorkflowRequest request, StreamObserver<SignalReceiptWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalReceiptWorkflow(request));
    }
}
