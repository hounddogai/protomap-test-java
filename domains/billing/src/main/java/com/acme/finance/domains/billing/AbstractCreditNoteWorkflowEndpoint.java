package com.acme.finance.domains.billing;

import com.acme.billing.v1.AdvanceCreditNoteWorkflowRequest;
import com.acme.billing.v1.AdvanceCreditNoteWorkflowResponse;
import com.acme.billing.v1.CancelCreditNoteWorkflowRequest;
import com.acme.billing.v1.CancelCreditNoteWorkflowResponse;
import com.acme.billing.v1.CreditNoteWorkflowServiceGrpc;
import com.acme.billing.v1.InspectCreditNoteWorkflowRequest;
import com.acme.billing.v1.InspectCreditNoteWorkflowResponse;
import com.acme.billing.v1.ListCreditNoteWorkflowsRequest;
import com.acme.billing.v1.ListCreditNoteWorkflowsResponse;
import com.acme.billing.v1.PauseCreditNoteWorkflowRequest;
import com.acme.billing.v1.PauseCreditNoteWorkflowResponse;
import com.acme.billing.v1.ResumeCreditNoteWorkflowRequest;
import com.acme.billing.v1.ResumeCreditNoteWorkflowResponse;
import com.acme.billing.v1.RetryCreditNoteWorkflowRequest;
import com.acme.billing.v1.RetryCreditNoteWorkflowResponse;
import com.acme.billing.v1.SignalCreditNoteWorkflowRequest;
import com.acme.billing.v1.SignalCreditNoteWorkflowResponse;
import com.acme.billing.v1.StartCreditNoteWorkflowRequest;
import com.acme.billing.v1.StartCreditNoteWorkflowResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Runs the long-lived workflow that takes a credit note from start to finish.
 */
public abstract class AbstractCreditNoteWorkflowEndpoint extends CreditNoteWorkflowServiceGrpc.CreditNoteWorkflowServiceImplBase {
    protected final CreditNoteWorkflowApplication application;

    protected AbstractCreditNoteWorkflowEndpoint(CreditNoteWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a credit note.
     */
    @Override
    public void startCreditNoteWorkflow(StartCreditNoteWorkflowRequest request, StreamObserver<StartCreditNoteWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startCreditNoteWorkflow(request));
    }

    /**
     * Advances a credit note.
     */
    @Override
    public void advanceCreditNoteWorkflow(AdvanceCreditNoteWorkflowRequest request, StreamObserver<AdvanceCreditNoteWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceCreditNoteWorkflow(request));
    }

    /**
     * Cancels a credit note.
     */
    @Override
    public void cancelCreditNoteWorkflow(CancelCreditNoteWorkflowRequest request, StreamObserver<CancelCreditNoteWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelCreditNoteWorkflow(request));
    }

    /**
     * Retrys a credit note.
     */
    @Override
    public void retryCreditNoteWorkflow(RetryCreditNoteWorkflowRequest request, StreamObserver<RetryCreditNoteWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryCreditNoteWorkflow(request));
    }

    /**
     * Pauses a credit note.
     */
    @Override
    public void pauseCreditNoteWorkflow(PauseCreditNoteWorkflowRequest request, StreamObserver<PauseCreditNoteWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseCreditNoteWorkflow(request));
    }

    /**
     * Resumes a credit note.
     */
    @Override
    public void resumeCreditNoteWorkflow(ResumeCreditNoteWorkflowRequest request, StreamObserver<ResumeCreditNoteWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeCreditNoteWorkflow(request));
    }

    /**
     * Returns a credit note.
     */
    @Override
    public void inspectCreditNoteWorkflow(InspectCreditNoteWorkflowRequest request, StreamObserver<InspectCreditNoteWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectCreditNoteWorkflow(request));
    }

    /**
     * Lists credit notes a page at a time.
     */
    @Override
    public void listCreditNoteWorkflows(ListCreditNoteWorkflowsRequest request, StreamObserver<ListCreditNoteWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listCreditNoteWorkflows(request));
    }

    /**
     * Signals a credit note.
     */
    @Override
    public void signalCreditNoteWorkflow(SignalCreditNoteWorkflowRequest request, StreamObserver<SignalCreditNoteWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalCreditNoteWorkflow(request));
    }
}
