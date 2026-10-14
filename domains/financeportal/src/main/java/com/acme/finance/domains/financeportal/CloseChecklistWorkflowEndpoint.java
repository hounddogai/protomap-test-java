package com.acme.finance.domains.financeportal;

import com.acme.finance.platform.RpcSupport;
import com.acme.financeportal.v1.AdvanceCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.AdvanceCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.CancelCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.CancelCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.CloseChecklistWorkflowServiceGrpc;
import com.acme.financeportal.v1.InspectCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.InspectCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.ListCloseChecklistWorkflowsRequest;
import com.acme.financeportal.v1.ListCloseChecklistWorkflowsResponse;
import com.acme.financeportal.v1.PauseCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.PauseCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.ResumeCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.ResumeCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.RetryCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.RetryCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.SignalCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.SignalCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.StartCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.StartCloseChecklistWorkflowResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes a close checklist from start to finish.
 */
@GrpcService
public class CloseChecklistWorkflowEndpoint extends CloseChecklistWorkflowServiceGrpc.CloseChecklistWorkflowServiceImplBase {
    protected final CloseChecklistWorkflowApplication application;

    public CloseChecklistWorkflowEndpoint(CloseChecklistWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a close checklist.
     */
    @Override
    public void startCloseChecklistWorkflow(StartCloseChecklistWorkflowRequest request, StreamObserver<StartCloseChecklistWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startCloseChecklistWorkflow(request));
    }

    /**
     * Advances a close checklist.
     */
    @Override
    public void advanceCloseChecklistWorkflow(AdvanceCloseChecklistWorkflowRequest request, StreamObserver<AdvanceCloseChecklistWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceCloseChecklistWorkflow(request));
    }

    /**
     * Cancels a close checklist.
     */
    @Override
    public void cancelCloseChecklistWorkflow(CancelCloseChecklistWorkflowRequest request, StreamObserver<CancelCloseChecklistWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelCloseChecklistWorkflow(request));
    }

    /**
     * Retrys a close checklist.
     */
    @Override
    public void retryCloseChecklistWorkflow(RetryCloseChecklistWorkflowRequest request, StreamObserver<RetryCloseChecklistWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryCloseChecklistWorkflow(request));
    }

    /**
     * Pauses a close checklist.
     */
    @Override
    public void pauseCloseChecklistWorkflow(PauseCloseChecklistWorkflowRequest request, StreamObserver<PauseCloseChecklistWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseCloseChecklistWorkflow(request));
    }

    /**
     * Resumes a close checklist.
     */
    @Override
    public void resumeCloseChecklistWorkflow(ResumeCloseChecklistWorkflowRequest request, StreamObserver<ResumeCloseChecklistWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeCloseChecklistWorkflow(request));
    }

    /**
     * Returns a close checklist.
     */
    @Override
    public void inspectCloseChecklistWorkflow(InspectCloseChecklistWorkflowRequest request, StreamObserver<InspectCloseChecklistWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectCloseChecklistWorkflow(request));
    }

    /**
     * Lists close checklists a page at a time.
     */
    @Override
    public void listCloseChecklistWorkflows(ListCloseChecklistWorkflowsRequest request, StreamObserver<ListCloseChecklistWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listCloseChecklistWorkflows(request));
    }

    /**
     * Signals a close checklist.
     */
    @Override
    public void signalCloseChecklistWorkflow(SignalCloseChecklistWorkflowRequest request, StreamObserver<SignalCloseChecklistWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalCloseChecklistWorkflow(request));
    }
}
