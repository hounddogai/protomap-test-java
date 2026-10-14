package com.acme.finance.domains.invoicing;

import com.acme.finance.platform.RpcSupport;
import com.acme.invoicing.v1.AdvanceInvoiceTemplateWorkflowRequest;
import com.acme.invoicing.v1.AdvanceInvoiceTemplateWorkflowResponse;
import com.acme.invoicing.v1.InspectInvoiceTemplateWorkflowRequest;
import com.acme.invoicing.v1.InspectInvoiceTemplateWorkflowResponse;
import com.acme.invoicing.v1.InvoiceTemplateWorkflowServiceGrpc;
import com.acme.invoicing.v1.ListInvoiceTemplateWorkflowsRequest;
import com.acme.invoicing.v1.ListInvoiceTemplateWorkflowsResponse;
import com.acme.invoicing.v1.PauseInvoiceTemplateWorkflowRequest;
import com.acme.invoicing.v1.PauseInvoiceTemplateWorkflowResponse;
import com.acme.invoicing.v1.ResumeInvoiceTemplateWorkflowRequest;
import com.acme.invoicing.v1.ResumeInvoiceTemplateWorkflowResponse;
import com.acme.invoicing.v1.RetryInvoiceTemplateWorkflowRequest;
import com.acme.invoicing.v1.RetryInvoiceTemplateWorkflowResponse;
import com.acme.invoicing.v1.SignalInvoiceTemplateWorkflowRequest;
import com.acme.invoicing.v1.SignalInvoiceTemplateWorkflowResponse;
import com.acme.invoicing.v1.StartInvoiceTemplateWorkflowRequest;
import com.acme.invoicing.v1.StartInvoiceTemplateWorkflowResponse;
import io.grpc.stub.StreamObserver;

/**
 * Runs the long-lived workflow that takes an invoice template from start to finish.
 */
public abstract class AbstractInvoiceTemplateWorkflowEndpoint extends InvoiceTemplateWorkflowServiceGrpc.InvoiceTemplateWorkflowServiceImplBase {
    protected final InvoiceTemplateWorkflowApplication application;

    protected AbstractInvoiceTemplateWorkflowEndpoint(InvoiceTemplateWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts an invoice template.
     */
    @Override
    public void startInvoiceTemplateWorkflow(StartInvoiceTemplateWorkflowRequest request, StreamObserver<StartInvoiceTemplateWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startInvoiceTemplateWorkflow(request));
    }

    /**
     * Advances an invoice template.
     */
    @Override
    public void advanceInvoiceTemplateWorkflow(AdvanceInvoiceTemplateWorkflowRequest request, StreamObserver<AdvanceInvoiceTemplateWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceInvoiceTemplateWorkflow(request));
    }

    /**
     * Retrys an invoice template.
     */
    @Override
    public void retryInvoiceTemplateWorkflow(RetryInvoiceTemplateWorkflowRequest request, StreamObserver<RetryInvoiceTemplateWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryInvoiceTemplateWorkflow(request));
    }

    /**
     * Pauses an invoice template.
     */
    @Override
    public void pauseInvoiceTemplateWorkflow(PauseInvoiceTemplateWorkflowRequest request, StreamObserver<PauseInvoiceTemplateWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseInvoiceTemplateWorkflow(request));
    }

    /**
     * Resumes an invoice template.
     */
    @Override
    public void resumeInvoiceTemplateWorkflow(ResumeInvoiceTemplateWorkflowRequest request, StreamObserver<ResumeInvoiceTemplateWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeInvoiceTemplateWorkflow(request));
    }

    /**
     * Returns an invoice template.
     */
    @Override
    public void inspectInvoiceTemplateWorkflow(InspectInvoiceTemplateWorkflowRequest request, StreamObserver<InspectInvoiceTemplateWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectInvoiceTemplateWorkflow(request));
    }

    /**
     * Lists invoice templates a page at a time.
     */
    @Override
    public void listInvoiceTemplateWorkflows(ListInvoiceTemplateWorkflowsRequest request, StreamObserver<ListInvoiceTemplateWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listInvoiceTemplateWorkflows(request));
    }

    /**
     * Signals an invoice template.
     */
    @Override
    public void signalInvoiceTemplateWorkflow(SignalInvoiceTemplateWorkflowRequest request, StreamObserver<SignalInvoiceTemplateWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalInvoiceTemplateWorkflow(request));
    }
}
