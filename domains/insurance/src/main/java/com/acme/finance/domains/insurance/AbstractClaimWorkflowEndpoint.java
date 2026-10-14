package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.AdvanceClaimWorkflowRequest;
import com.acme.insurance.v1.AdvanceClaimWorkflowResponse;
import com.acme.insurance.v1.CancelClaimWorkflowRequest;
import com.acme.insurance.v1.CancelClaimWorkflowResponse;
import com.acme.insurance.v1.ClaimWorkflowServiceGrpc;
import com.acme.insurance.v1.InspectClaimWorkflowRequest;
import com.acme.insurance.v1.InspectClaimWorkflowResponse;
import com.acme.insurance.v1.ListClaimWorkflowsRequest;
import com.acme.insurance.v1.ListClaimWorkflowsResponse;
import com.acme.insurance.v1.PauseClaimWorkflowRequest;
import com.acme.insurance.v1.PauseClaimWorkflowResponse;
import com.acme.insurance.v1.ResumeClaimWorkflowRequest;
import com.acme.insurance.v1.ResumeClaimWorkflowResponse;
import com.acme.insurance.v1.RetryClaimWorkflowRequest;
import com.acme.insurance.v1.RetryClaimWorkflowResponse;
import com.acme.insurance.v1.SignalClaimWorkflowRequest;
import com.acme.insurance.v1.SignalClaimWorkflowResponse;
import com.acme.insurance.v1.StartClaimWorkflowRequest;
import com.acme.insurance.v1.StartClaimWorkflowResponse;
import io.grpc.stub.StreamObserver;

/**
 * Runs the long-lived workflow that takes a claim from start to finish.
 */
public abstract class AbstractClaimWorkflowEndpoint extends ClaimWorkflowServiceGrpc.ClaimWorkflowServiceImplBase {
    protected final ClaimWorkflowApplication application;

    protected AbstractClaimWorkflowEndpoint(ClaimWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a claim.
     */
    @Override
    public void startClaimWorkflow(StartClaimWorkflowRequest request, StreamObserver<StartClaimWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startClaimWorkflow(request));
    }

    /**
     * Advances a claim.
     */
    @Override
    public void advanceClaimWorkflow(AdvanceClaimWorkflowRequest request, StreamObserver<AdvanceClaimWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceClaimWorkflow(request));
    }

    /**
     * Cancels a claim.
     */
    @Override
    public void cancelClaimWorkflow(CancelClaimWorkflowRequest request, StreamObserver<CancelClaimWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelClaimWorkflow(request));
    }

    /**
     * Retrys a claim.
     */
    @Override
    public void retryClaimWorkflow(RetryClaimWorkflowRequest request, StreamObserver<RetryClaimWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryClaimWorkflow(request));
    }

    /**
     * Pauses a claim.
     */
    @Override
    public void pauseClaimWorkflow(PauseClaimWorkflowRequest request, StreamObserver<PauseClaimWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseClaimWorkflow(request));
    }

    /**
     * Resumes a claim.
     */
    @Override
    public void resumeClaimWorkflow(ResumeClaimWorkflowRequest request, StreamObserver<ResumeClaimWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeClaimWorkflow(request));
    }

    /**
     * Returns a claim.
     */
    @Override
    public void inspectClaimWorkflow(InspectClaimWorkflowRequest request, StreamObserver<InspectClaimWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectClaimWorkflow(request));
    }

    /**
     * Lists claims a page at a time.
     */
    @Override
    public void listClaimWorkflows(ListClaimWorkflowsRequest request, StreamObserver<ListClaimWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listClaimWorkflows(request));
    }

    /**
     * Signals a claim.
     */
    @Override
    public void signalClaimWorkflow(SignalClaimWorkflowRequest request, StreamObserver<SignalClaimWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalClaimWorkflow(request));
    }
}
