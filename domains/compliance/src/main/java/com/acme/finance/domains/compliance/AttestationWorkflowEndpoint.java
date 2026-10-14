package com.acme.finance.domains.compliance;

import com.acme.compliance.v1.AdvanceAttestationWorkflowRequest;
import com.acme.compliance.v1.AdvanceAttestationWorkflowResponse;
import com.acme.compliance.v1.AttestationWorkflowServiceGrpc;
import com.acme.compliance.v1.CancelAttestationWorkflowRequest;
import com.acme.compliance.v1.CancelAttestationWorkflowResponse;
import com.acme.compliance.v1.InspectAttestationWorkflowRequest;
import com.acme.compliance.v1.InspectAttestationWorkflowResponse;
import com.acme.compliance.v1.ListAttestationWorkflowsRequest;
import com.acme.compliance.v1.ListAttestationWorkflowsResponse;
import com.acme.compliance.v1.PauseAttestationWorkflowRequest;
import com.acme.compliance.v1.PauseAttestationWorkflowResponse;
import com.acme.compliance.v1.ResumeAttestationWorkflowRequest;
import com.acme.compliance.v1.ResumeAttestationWorkflowResponse;
import com.acme.compliance.v1.RetryAttestationWorkflowRequest;
import com.acme.compliance.v1.RetryAttestationWorkflowResponse;
import com.acme.compliance.v1.SignalAttestationWorkflowRequest;
import com.acme.compliance.v1.SignalAttestationWorkflowResponse;
import com.acme.compliance.v1.StartAttestationWorkflowRequest;
import com.acme.compliance.v1.StartAttestationWorkflowResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes an attestation from start to finish.
 */
@GrpcService
public class AttestationWorkflowEndpoint extends AttestationWorkflowServiceGrpc.AttestationWorkflowServiceImplBase {
    protected final AttestationWorkflowApplication application;

    public AttestationWorkflowEndpoint(AttestationWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts an attestation.
     */
    @Override
    public void startAttestationWorkflow(StartAttestationWorkflowRequest request, StreamObserver<StartAttestationWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startAttestationWorkflow(request));
    }

    /**
     * Advances an attestation.
     */
    @Override
    public void advanceAttestationWorkflow(AdvanceAttestationWorkflowRequest request, StreamObserver<AdvanceAttestationWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceAttestationWorkflow(request));
    }

    /**
     * Cancels an attestation.
     */
    @Override
    public void cancelAttestationWorkflow(CancelAttestationWorkflowRequest request, StreamObserver<CancelAttestationWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelAttestationWorkflow(request));
    }

    /**
     * Retrys an attestation.
     */
    @Override
    public void retryAttestationWorkflow(RetryAttestationWorkflowRequest request, StreamObserver<RetryAttestationWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryAttestationWorkflow(request));
    }

    /**
     * Pauses an attestation.
     */
    @Override
    public void pauseAttestationWorkflow(PauseAttestationWorkflowRequest request, StreamObserver<PauseAttestationWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseAttestationWorkflow(request));
    }

    /**
     * Resumes an attestation.
     */
    @Override
    public void resumeAttestationWorkflow(ResumeAttestationWorkflowRequest request, StreamObserver<ResumeAttestationWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeAttestationWorkflow(request));
    }

    /**
     * Returns an attestation.
     */
    @Override
    public void inspectAttestationWorkflow(InspectAttestationWorkflowRequest request, StreamObserver<InspectAttestationWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectAttestationWorkflow(request));
    }

    /**
     * Lists attestations a page at a time.
     */
    @Override
    public void listAttestationWorkflows(ListAttestationWorkflowsRequest request, StreamObserver<ListAttestationWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listAttestationWorkflows(request));
    }

    /**
     * Signals an attestation.
     */
    @Override
    public void signalAttestationWorkflow(SignalAttestationWorkflowRequest request, StreamObserver<SignalAttestationWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalAttestationWorkflow(request));
    }
}
