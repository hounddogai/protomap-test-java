package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.AdvanceCoverageWorkflowRequest;
import com.acme.insurance.v1.AdvanceCoverageWorkflowResponse;
import com.acme.insurance.v1.CancelCoverageWorkflowRequest;
import com.acme.insurance.v1.CancelCoverageWorkflowResponse;
import com.acme.insurance.v1.CoverageWorkflowServiceGrpc;
import com.acme.insurance.v1.InspectCoverageWorkflowRequest;
import com.acme.insurance.v1.InspectCoverageWorkflowResponse;
import com.acme.insurance.v1.ListCoverageWorkflowsRequest;
import com.acme.insurance.v1.ListCoverageWorkflowsResponse;
import com.acme.insurance.v1.PauseCoverageWorkflowRequest;
import com.acme.insurance.v1.PauseCoverageWorkflowResponse;
import com.acme.insurance.v1.ResumeCoverageWorkflowRequest;
import com.acme.insurance.v1.ResumeCoverageWorkflowResponse;
import com.acme.insurance.v1.RetryCoverageWorkflowRequest;
import com.acme.insurance.v1.RetryCoverageWorkflowResponse;
import com.acme.insurance.v1.SignalCoverageWorkflowRequest;
import com.acme.insurance.v1.SignalCoverageWorkflowResponse;
import com.acme.insurance.v1.StartCoverageWorkflowRequest;
import com.acme.insurance.v1.StartCoverageWorkflowResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes a coverage from start to finish.
 */
@GrpcService
public class CoverageWorkflowEndpoint extends CoverageWorkflowServiceGrpc.CoverageWorkflowServiceImplBase {
    protected final CoverageWorkflowApplication application;

    public CoverageWorkflowEndpoint(CoverageWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a coverage.
     */
    @Override
    public void startCoverageWorkflow(StartCoverageWorkflowRequest request, StreamObserver<StartCoverageWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startCoverageWorkflow(request));
    }

    /**
     * Advances a coverage.
     */
    @Override
    public void advanceCoverageWorkflow(AdvanceCoverageWorkflowRequest request, StreamObserver<AdvanceCoverageWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceCoverageWorkflow(request));
    }

    /**
     * Cancels a coverage.
     */
    @Override
    public void cancelCoverageWorkflow(CancelCoverageWorkflowRequest request, StreamObserver<CancelCoverageWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelCoverageWorkflow(request));
    }

    /**
     * Retrys a coverage.
     */
    @Override
    public void retryCoverageWorkflow(RetryCoverageWorkflowRequest request, StreamObserver<RetryCoverageWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryCoverageWorkflow(request));
    }

    /**
     * Pauses a coverage.
     */
    @Override
    public void pauseCoverageWorkflow(PauseCoverageWorkflowRequest request, StreamObserver<PauseCoverageWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseCoverageWorkflow(request));
    }

    /**
     * Resumes a coverage.
     */
    @Override
    public void resumeCoverageWorkflow(ResumeCoverageWorkflowRequest request, StreamObserver<ResumeCoverageWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeCoverageWorkflow(request));
    }

    /**
     * Returns a coverage.
     */
    @Override
    public void inspectCoverageWorkflow(InspectCoverageWorkflowRequest request, StreamObserver<InspectCoverageWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectCoverageWorkflow(request));
    }

    /**
     * Lists coverages a page at a time.
     */
    @Override
    public void listCoverageWorkflows(ListCoverageWorkflowsRequest request, StreamObserver<ListCoverageWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listCoverageWorkflows(request));
    }

    /**
     * Signals a coverage.
     */
    @Override
    public void signalCoverageWorkflow(SignalCoverageWorkflowRequest request, StreamObserver<SignalCoverageWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalCoverageWorkflow(request));
    }
}
