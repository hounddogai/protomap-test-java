package com.acme.finance.domains.billing;

import com.acme.billing.v1.AdvanceInvoiceWorkflowRequest;
import com.acme.billing.v1.AdvanceInvoiceWorkflowResponse;
import com.acme.billing.v1.CancelInvoiceWorkflowRequest;
import com.acme.billing.v1.CancelInvoiceWorkflowResponse;
import com.acme.billing.v1.InspectInvoiceWorkflowRequest;
import com.acme.billing.v1.InspectInvoiceWorkflowResponse;
import com.acme.billing.v1.InvoiceWorkflowServiceGrpc;
import com.acme.billing.v1.ListInvoiceWorkflowsRequest;
import com.acme.billing.v1.ListInvoiceWorkflowsResponse;
import com.acme.billing.v1.PauseInvoiceWorkflowRequest;
import com.acme.billing.v1.PauseInvoiceWorkflowResponse;
import com.acme.billing.v1.ResumeInvoiceWorkflowRequest;
import com.acme.billing.v1.ResumeInvoiceWorkflowResponse;
import com.acme.billing.v1.RetryInvoiceWorkflowRequest;
import com.acme.billing.v1.RetryInvoiceWorkflowResponse;
import com.acme.billing.v1.SignalInvoiceWorkflowRequest;
import com.acme.billing.v1.SignalInvoiceWorkflowResponse;
import com.acme.billing.v1.StartInvoiceWorkflowRequest;
import com.acme.billing.v1.StartInvoiceWorkflowResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes an invoice from start to finish.
 */
@GrpcService
public class InvoiceWorkflowEndpoint extends InvoiceWorkflowServiceGrpc.InvoiceWorkflowServiceImplBase {
    protected final InvoiceWorkflowApplication application;

    public InvoiceWorkflowEndpoint(InvoiceWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts an invoice.
     */
    @Override
    public void startInvoiceWorkflow(StartInvoiceWorkflowRequest request, StreamObserver<StartInvoiceWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startInvoiceWorkflow(request));
    }

    /**
     * Advances an invoice.
     */
    @Override
    public void advanceInvoiceWorkflow(AdvanceInvoiceWorkflowRequest request, StreamObserver<AdvanceInvoiceWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceInvoiceWorkflow(request));
    }

    /**
     * Cancels an invoice.
     */
    @Override
    public void cancelInvoiceWorkflow(CancelInvoiceWorkflowRequest request, StreamObserver<CancelInvoiceWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelInvoiceWorkflow(request));
    }

    /**
     * Retrys an invoice.
     */
    @Override
    public void retryInvoiceWorkflow(RetryInvoiceWorkflowRequest request, StreamObserver<RetryInvoiceWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryInvoiceWorkflow(request));
    }

    /**
     * Pauses an invoice.
     */
    @Override
    public void pauseInvoiceWorkflow(PauseInvoiceWorkflowRequest request, StreamObserver<PauseInvoiceWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseInvoiceWorkflow(request));
    }

    /**
     * Resumes an invoice.
     */
    @Override
    public void resumeInvoiceWorkflow(ResumeInvoiceWorkflowRequest request, StreamObserver<ResumeInvoiceWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeInvoiceWorkflow(request));
    }

    /**
     * Returns an invoice.
     */
    @Override
    public void inspectInvoiceWorkflow(InspectInvoiceWorkflowRequest request, StreamObserver<InspectInvoiceWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectInvoiceWorkflow(request));
    }

    /**
     * Lists invoices a page at a time.
     */
    @Override
    public void listInvoiceWorkflows(ListInvoiceWorkflowsRequest request, StreamObserver<ListInvoiceWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listInvoiceWorkflows(request));
    }

    /**
     * Signals an invoice.
     */
    @Override
    public void signalInvoiceWorkflow(SignalInvoiceWorkflowRequest request, StreamObserver<SignalInvoiceWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalInvoiceWorkflow(request));
    }
}
