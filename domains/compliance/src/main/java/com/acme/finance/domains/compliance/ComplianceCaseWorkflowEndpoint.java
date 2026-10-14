package com.acme.finance.domains.compliance;

import com.acme.compliance.v1.AdvanceComplianceCaseWorkflowRequest;
import com.acme.compliance.v1.AdvanceComplianceCaseWorkflowResponse;
import com.acme.compliance.v1.CancelComplianceCaseWorkflowRequest;
import com.acme.compliance.v1.CancelComplianceCaseWorkflowResponse;
import com.acme.compliance.v1.ComplianceCaseWorkflowServiceGrpc;
import com.acme.compliance.v1.InspectComplianceCaseWorkflowRequest;
import com.acme.compliance.v1.InspectComplianceCaseWorkflowResponse;
import com.acme.compliance.v1.ListComplianceCaseWorkflowsRequest;
import com.acme.compliance.v1.ListComplianceCaseWorkflowsResponse;
import com.acme.compliance.v1.PauseComplianceCaseWorkflowRequest;
import com.acme.compliance.v1.PauseComplianceCaseWorkflowResponse;
import com.acme.compliance.v1.ResumeComplianceCaseWorkflowRequest;
import com.acme.compliance.v1.ResumeComplianceCaseWorkflowResponse;
import com.acme.compliance.v1.RetryComplianceCaseWorkflowRequest;
import com.acme.compliance.v1.RetryComplianceCaseWorkflowResponse;
import com.acme.compliance.v1.StartComplianceCaseWorkflowRequest;
import com.acme.compliance.v1.StartComplianceCaseWorkflowResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes a compliance case from start to finish.
 */
@GrpcService
public class ComplianceCaseWorkflowEndpoint extends ComplianceCaseWorkflowServiceGrpc.ComplianceCaseWorkflowServiceImplBase {
    protected final ComplianceCaseWorkflowApplication application;

    public ComplianceCaseWorkflowEndpoint(ComplianceCaseWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a compliance case.
     */
    @Override
    public void startComplianceCaseWorkflow(StartComplianceCaseWorkflowRequest request, StreamObserver<StartComplianceCaseWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startComplianceCaseWorkflow(request));
    }

    /**
     * Advances a compliance case.
     */
    @Override
    public void advanceComplianceCaseWorkflow(AdvanceComplianceCaseWorkflowRequest request, StreamObserver<AdvanceComplianceCaseWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceComplianceCaseWorkflow(request));
    }

    /**
     * Cancels a compliance case.
     */
    @Override
    public void cancelComplianceCaseWorkflow(CancelComplianceCaseWorkflowRequest request, StreamObserver<CancelComplianceCaseWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelComplianceCaseWorkflow(request));
    }

    /**
     * Retrys a compliance case.
     */
    @Override
    public void retryComplianceCaseWorkflow(RetryComplianceCaseWorkflowRequest request, StreamObserver<RetryComplianceCaseWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryComplianceCaseWorkflow(request));
    }

    /**
     * Pauses a compliance case.
     */
    @Override
    public void pauseComplianceCaseWorkflow(PauseComplianceCaseWorkflowRequest request, StreamObserver<PauseComplianceCaseWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseComplianceCaseWorkflow(request));
    }

    /**
     * Resumes a compliance case.
     */
    @Override
    public void resumeComplianceCaseWorkflow(ResumeComplianceCaseWorkflowRequest request, StreamObserver<ResumeComplianceCaseWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeComplianceCaseWorkflow(request));
    }

    /**
     * Returns a compliance case.
     */
    @Override
    public void inspectComplianceCaseWorkflow(InspectComplianceCaseWorkflowRequest request, StreamObserver<InspectComplianceCaseWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectComplianceCaseWorkflow(request));
    }

    /**
     * Lists compliance cases a page at a time.
     */
    @Override
    public void listComplianceCaseWorkflows(ListComplianceCaseWorkflowsRequest request, StreamObserver<ListComplianceCaseWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listComplianceCaseWorkflows(request));
    }
}
