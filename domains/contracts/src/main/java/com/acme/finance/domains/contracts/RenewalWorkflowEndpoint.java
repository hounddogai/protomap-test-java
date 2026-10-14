package com.acme.finance.domains.contracts;

import com.acme.contracts.v1.AdvanceRenewalWorkflowRequest;
import com.acme.contracts.v1.AdvanceRenewalWorkflowResponse;
import com.acme.contracts.v1.CancelRenewalWorkflowRequest;
import com.acme.contracts.v1.CancelRenewalWorkflowResponse;
import com.acme.contracts.v1.InspectRenewalWorkflowRequest;
import com.acme.contracts.v1.InspectRenewalWorkflowResponse;
import com.acme.contracts.v1.ListRenewalWorkflowsRequest;
import com.acme.contracts.v1.ListRenewalWorkflowsResponse;
import com.acme.contracts.v1.PauseRenewalWorkflowRequest;
import com.acme.contracts.v1.PauseRenewalWorkflowResponse;
import com.acme.contracts.v1.RenewalWorkflowServiceGrpc;
import com.acme.contracts.v1.ResumeRenewalWorkflowRequest;
import com.acme.contracts.v1.ResumeRenewalWorkflowResponse;
import com.acme.contracts.v1.RetryRenewalWorkflowRequest;
import com.acme.contracts.v1.RetryRenewalWorkflowResponse;
import com.acme.contracts.v1.StartRenewalWorkflowRequest;
import com.acme.contracts.v1.StartRenewalWorkflowResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes a renewal from start to finish.
 */
@GrpcService
public class RenewalWorkflowEndpoint extends RenewalWorkflowServiceGrpc.RenewalWorkflowServiceImplBase {
    protected final RenewalWorkflowApplication application;

    public RenewalWorkflowEndpoint(RenewalWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a renewal.
     */
    @Override
    public void startRenewalWorkflow(StartRenewalWorkflowRequest request, StreamObserver<StartRenewalWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startRenewalWorkflow(request));
    }

    /**
     * Advances a renewal.
     */
    @Override
    public void advanceRenewalWorkflow(AdvanceRenewalWorkflowRequest request, StreamObserver<AdvanceRenewalWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceRenewalWorkflow(request));
    }

    /**
     * Cancels a renewal.
     */
    @Override
    public void cancelRenewalWorkflow(CancelRenewalWorkflowRequest request, StreamObserver<CancelRenewalWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelRenewalWorkflow(request));
    }

    /**
     * Retrys a renewal.
     */
    @Override
    public void retryRenewalWorkflow(RetryRenewalWorkflowRequest request, StreamObserver<RetryRenewalWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryRenewalWorkflow(request));
    }

    /**
     * Pauses a renewal.
     */
    @Override
    public void pauseRenewalWorkflow(PauseRenewalWorkflowRequest request, StreamObserver<PauseRenewalWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseRenewalWorkflow(request));
    }

    /**
     * Resumes a renewal.
     */
    @Override
    public void resumeRenewalWorkflow(ResumeRenewalWorkflowRequest request, StreamObserver<ResumeRenewalWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeRenewalWorkflow(request));
    }

    /**
     * Returns a renewal.
     */
    @Override
    public void inspectRenewalWorkflow(InspectRenewalWorkflowRequest request, StreamObserver<InspectRenewalWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectRenewalWorkflow(request));
    }

    /**
     * Lists renewals a page at a time.
     */
    @Override
    public void listRenewalWorkflows(ListRenewalWorkflowsRequest request, StreamObserver<ListRenewalWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listRenewalWorkflows(request));
    }
}
