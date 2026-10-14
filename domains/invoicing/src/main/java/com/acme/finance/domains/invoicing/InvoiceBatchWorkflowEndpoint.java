package com.acme.finance.domains.invoicing;

import com.acme.finance.platform.RpcSupport;
import com.acme.invoicing.v1.AdvanceInvoiceBatchWorkflowRequest;
import com.acme.invoicing.v1.AdvanceInvoiceBatchWorkflowResponse;
import com.acme.invoicing.v1.CancelInvoiceBatchWorkflowRequest;
import com.acme.invoicing.v1.CancelInvoiceBatchWorkflowResponse;
import com.acme.invoicing.v1.InspectInvoiceBatchWorkflowRequest;
import com.acme.invoicing.v1.InspectInvoiceBatchWorkflowResponse;
import com.acme.invoicing.v1.InvoiceBatchWorkflowServiceGrpc;
import com.acme.invoicing.v1.ListInvoiceBatchWorkflowsRequest;
import com.acme.invoicing.v1.ListInvoiceBatchWorkflowsResponse;
import com.acme.invoicing.v1.PauseInvoiceBatchWorkflowRequest;
import com.acme.invoicing.v1.PauseInvoiceBatchWorkflowResponse;
import com.acme.invoicing.v1.ResumeInvoiceBatchWorkflowRequest;
import com.acme.invoicing.v1.ResumeInvoiceBatchWorkflowResponse;
import com.acme.invoicing.v1.RetryInvoiceBatchWorkflowRequest;
import com.acme.invoicing.v1.RetryInvoiceBatchWorkflowResponse;
import com.acme.invoicing.v1.StartInvoiceBatchWorkflowRequest;
import com.acme.invoicing.v1.StartInvoiceBatchWorkflowResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes an invoice batch from start to finish.
 */
@GrpcService
public class InvoiceBatchWorkflowEndpoint extends InvoiceBatchWorkflowServiceGrpc.InvoiceBatchWorkflowServiceImplBase {
    protected final InvoiceBatchWorkflowApplication application;

    public InvoiceBatchWorkflowEndpoint(InvoiceBatchWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts an invoice batch.
     */
    @Override
    public void startInvoiceBatchWorkflow(StartInvoiceBatchWorkflowRequest request, StreamObserver<StartInvoiceBatchWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startInvoiceBatchWorkflow(request));
    }

    /**
     * Advances an invoice batch.
     */
    @Override
    public void advanceInvoiceBatchWorkflow(AdvanceInvoiceBatchWorkflowRequest request, StreamObserver<AdvanceInvoiceBatchWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceInvoiceBatchWorkflow(request));
    }

    /**
     * Cancels an invoice batch.
     */
    @Override
    public void cancelInvoiceBatchWorkflow(CancelInvoiceBatchWorkflowRequest request, StreamObserver<CancelInvoiceBatchWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelInvoiceBatchWorkflow(request));
    }

    /**
     * Retrys an invoice batch.
     */
    @Override
    public void retryInvoiceBatchWorkflow(RetryInvoiceBatchWorkflowRequest request, StreamObserver<RetryInvoiceBatchWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryInvoiceBatchWorkflow(request));
    }

    /**
     * Pauses an invoice batch.
     */
    @Override
    public void pauseInvoiceBatchWorkflow(PauseInvoiceBatchWorkflowRequest request, StreamObserver<PauseInvoiceBatchWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseInvoiceBatchWorkflow(request));
    }

    /**
     * Resumes an invoice batch.
     */
    @Override
    public void resumeInvoiceBatchWorkflow(ResumeInvoiceBatchWorkflowRequest request, StreamObserver<ResumeInvoiceBatchWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeInvoiceBatchWorkflow(request));
    }

    /**
     * Returns an invoice batch.
     */
    @Override
    public void inspectInvoiceBatchWorkflow(InspectInvoiceBatchWorkflowRequest request, StreamObserver<InspectInvoiceBatchWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectInvoiceBatchWorkflow(request));
    }

    /**
     * Lists invoice batches a page at a time.
     */
    @Override
    public void listInvoiceBatchWorkflows(ListInvoiceBatchWorkflowsRequest request, StreamObserver<ListInvoiceBatchWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listInvoiceBatchWorkflows(request));
    }
}
