package com.acme.finance.domains.financeportal;

import com.acme.finance.platform.RpcSupport;
import com.acme.financeportal.v1.AdvanceApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.AdvanceApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.ApprovalInboxWorkflowServiceGrpc;
import com.acme.financeportal.v1.CancelApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.CancelApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.InspectApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.InspectApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.ListApprovalInboxWorkflowsRequest;
import com.acme.financeportal.v1.ListApprovalInboxWorkflowsResponse;
import com.acme.financeportal.v1.PauseApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.PauseApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.ResumeApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.ResumeApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.RetryApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.RetryApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.SignalApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.SignalApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.StartApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.StartApprovalInboxWorkflowResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes an approval inbox from start to finish.
 */
@GrpcService
public class ApprovalInboxWorkflowEndpoint extends ApprovalInboxWorkflowServiceGrpc.ApprovalInboxWorkflowServiceImplBase {
    protected final ApprovalInboxWorkflowApplication application;

    public ApprovalInboxWorkflowEndpoint(ApprovalInboxWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts an approval inbox.
     */
    @Override
    public void startApprovalInboxWorkflow(StartApprovalInboxWorkflowRequest request, StreamObserver<StartApprovalInboxWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startApprovalInboxWorkflow(request));
    }

    /**
     * Advances an approval inbox.
     */
    @Override
    public void advanceApprovalInboxWorkflow(AdvanceApprovalInboxWorkflowRequest request, StreamObserver<AdvanceApprovalInboxWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceApprovalInboxWorkflow(request));
    }

    /**
     * Cancels an approval inbox.
     */
    @Override
    public void cancelApprovalInboxWorkflow(CancelApprovalInboxWorkflowRequest request, StreamObserver<CancelApprovalInboxWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelApprovalInboxWorkflow(request));
    }

    /**
     * Retrys an approval inbox.
     */
    @Override
    public void retryApprovalInboxWorkflow(RetryApprovalInboxWorkflowRequest request, StreamObserver<RetryApprovalInboxWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryApprovalInboxWorkflow(request));
    }

    /**
     * Pauses an approval inbox.
     */
    @Override
    public void pauseApprovalInboxWorkflow(PauseApprovalInboxWorkflowRequest request, StreamObserver<PauseApprovalInboxWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseApprovalInboxWorkflow(request));
    }

    /**
     * Resumes an approval inbox.
     */
    @Override
    public void resumeApprovalInboxWorkflow(ResumeApprovalInboxWorkflowRequest request, StreamObserver<ResumeApprovalInboxWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeApprovalInboxWorkflow(request));
    }

    /**
     * Returns an approval inbox.
     */
    @Override
    public void inspectApprovalInboxWorkflow(InspectApprovalInboxWorkflowRequest request, StreamObserver<InspectApprovalInboxWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectApprovalInboxWorkflow(request));
    }

    /**
     * Lists approval inboxes a page at a time.
     */
    @Override
    public void listApprovalInboxWorkflows(ListApprovalInboxWorkflowsRequest request, StreamObserver<ListApprovalInboxWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listApprovalInboxWorkflows(request));
    }

    /**
     * Signals an approval inbox.
     */
    @Override
    public void signalApprovalInboxWorkflow(SignalApprovalInboxWorkflowRequest request, StreamObserver<SignalApprovalInboxWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalApprovalInboxWorkflow(request));
    }
}
