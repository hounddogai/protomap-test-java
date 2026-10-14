package com.acme.finance.domains.invoicing;

import com.acme.finance.platform.RpcSupport;
import com.acme.invoicing.v1.AdvanceEInvoiceWorkflowRequest;
import com.acme.invoicing.v1.AdvanceEInvoiceWorkflowResponse;
import com.acme.invoicing.v1.CancelEInvoiceWorkflowRequest;
import com.acme.invoicing.v1.CancelEInvoiceWorkflowResponse;
import com.acme.invoicing.v1.EInvoiceWorkflowServiceGrpc;
import com.acme.invoicing.v1.InspectEInvoiceWorkflowRequest;
import com.acme.invoicing.v1.InspectEInvoiceWorkflowResponse;
import com.acme.invoicing.v1.ListEInvoiceWorkflowsRequest;
import com.acme.invoicing.v1.ListEInvoiceWorkflowsResponse;
import com.acme.invoicing.v1.PauseEInvoiceWorkflowRequest;
import com.acme.invoicing.v1.PauseEInvoiceWorkflowResponse;
import com.acme.invoicing.v1.ResumeEInvoiceWorkflowRequest;
import com.acme.invoicing.v1.ResumeEInvoiceWorkflowResponse;
import com.acme.invoicing.v1.RetryEInvoiceWorkflowRequest;
import com.acme.invoicing.v1.RetryEInvoiceWorkflowResponse;
import com.acme.invoicing.v1.SignalEInvoiceWorkflowRequest;
import com.acme.invoicing.v1.SignalEInvoiceWorkflowResponse;
import com.acme.invoicing.v1.StartEInvoiceWorkflowRequest;
import com.acme.invoicing.v1.StartEInvoiceWorkflowResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes an e invoice from start to finish.
 */
@GrpcService
public class EInvoiceWorkflowEndpoint extends EInvoiceWorkflowServiceGrpc.EInvoiceWorkflowServiceImplBase {
    protected final EInvoiceWorkflowApplication application;

    public EInvoiceWorkflowEndpoint(EInvoiceWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts an e invoice.
     */
    @Override
    public void startEInvoiceWorkflow(StartEInvoiceWorkflowRequest request, StreamObserver<StartEInvoiceWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startEInvoiceWorkflow(request));
    }

    /**
     * Advances an e invoice.
     */
    @Override
    public void advanceEInvoiceWorkflow(AdvanceEInvoiceWorkflowRequest request, StreamObserver<AdvanceEInvoiceWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceEInvoiceWorkflow(request));
    }

    /**
     * Cancels an e invoice.
     */
    @Override
    public void cancelEInvoiceWorkflow(CancelEInvoiceWorkflowRequest request, StreamObserver<CancelEInvoiceWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelEInvoiceWorkflow(request));
    }

    /**
     * Retrys an e invoice.
     */
    @Override
    public void retryEInvoiceWorkflow(RetryEInvoiceWorkflowRequest request, StreamObserver<RetryEInvoiceWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryEInvoiceWorkflow(request));
    }

    /**
     * Pauses an e invoice.
     */
    @Override
    public void pauseEInvoiceWorkflow(PauseEInvoiceWorkflowRequest request, StreamObserver<PauseEInvoiceWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseEInvoiceWorkflow(request));
    }

    /**
     * Resumes an e invoice.
     */
    @Override
    public void resumeEInvoiceWorkflow(ResumeEInvoiceWorkflowRequest request, StreamObserver<ResumeEInvoiceWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeEInvoiceWorkflow(request));
    }

    /**
     * Returns an e invoice.
     */
    @Override
    public void inspectEInvoiceWorkflow(InspectEInvoiceWorkflowRequest request, StreamObserver<InspectEInvoiceWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectEInvoiceWorkflow(request));
    }

    /**
     * Lists e invoices a page at a time.
     */
    @Override
    public void listEInvoiceWorkflows(ListEInvoiceWorkflowsRequest request, StreamObserver<ListEInvoiceWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listEInvoiceWorkflows(request));
    }

    /**
     * Signals an e invoice.
     */
    @Override
    public void signalEInvoiceWorkflow(SignalEInvoiceWorkflowRequest request, StreamObserver<SignalEInvoiceWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalEInvoiceWorkflow(request));
    }
}
