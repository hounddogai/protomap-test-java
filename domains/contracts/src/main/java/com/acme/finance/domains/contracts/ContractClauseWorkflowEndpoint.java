package com.acme.finance.domains.contracts;

import com.acme.contracts.v1.AdvanceContractClauseWorkflowRequest;
import com.acme.contracts.v1.AdvanceContractClauseWorkflowResponse;
import com.acme.contracts.v1.CancelContractClauseWorkflowRequest;
import com.acme.contracts.v1.CancelContractClauseWorkflowResponse;
import com.acme.contracts.v1.ContractClauseWorkflowServiceGrpc;
import com.acme.contracts.v1.InspectContractClauseWorkflowRequest;
import com.acme.contracts.v1.InspectContractClauseWorkflowResponse;
import com.acme.contracts.v1.ListContractClauseWorkflowsRequest;
import com.acme.contracts.v1.ListContractClauseWorkflowsResponse;
import com.acme.contracts.v1.PauseContractClauseWorkflowRequest;
import com.acme.contracts.v1.PauseContractClauseWorkflowResponse;
import com.acme.contracts.v1.ResumeContractClauseWorkflowRequest;
import com.acme.contracts.v1.ResumeContractClauseWorkflowResponse;
import com.acme.contracts.v1.RetryContractClauseWorkflowRequest;
import com.acme.contracts.v1.RetryContractClauseWorkflowResponse;
import com.acme.contracts.v1.SignalContractClauseWorkflowRequest;
import com.acme.contracts.v1.SignalContractClauseWorkflowResponse;
import com.acme.contracts.v1.StartContractClauseWorkflowRequest;
import com.acme.contracts.v1.StartContractClauseWorkflowResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes a contract clause from start to finish.
 */
@GrpcService
public class ContractClauseWorkflowEndpoint extends ContractClauseWorkflowServiceGrpc.ContractClauseWorkflowServiceImplBase {
    protected final ContractClauseWorkflowApplication application;

    public ContractClauseWorkflowEndpoint(ContractClauseWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a contract clause.
     */
    @Override
    public void startContractClauseWorkflow(StartContractClauseWorkflowRequest request, StreamObserver<StartContractClauseWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startContractClauseWorkflow(request));
    }

    /**
     * Advances a contract clause.
     */
    @Override
    public void advanceContractClauseWorkflow(AdvanceContractClauseWorkflowRequest request, StreamObserver<AdvanceContractClauseWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceContractClauseWorkflow(request));
    }

    /**
     * Cancels a contract clause.
     */
    @Override
    public void cancelContractClauseWorkflow(CancelContractClauseWorkflowRequest request, StreamObserver<CancelContractClauseWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelContractClauseWorkflow(request));
    }

    /**
     * Retrys a contract clause.
     */
    @Override
    public void retryContractClauseWorkflow(RetryContractClauseWorkflowRequest request, StreamObserver<RetryContractClauseWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryContractClauseWorkflow(request));
    }

    /**
     * Pauses a contract clause.
     */
    @Override
    public void pauseContractClauseWorkflow(PauseContractClauseWorkflowRequest request, StreamObserver<PauseContractClauseWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseContractClauseWorkflow(request));
    }

    /**
     * Resumes a contract clause.
     */
    @Override
    public void resumeContractClauseWorkflow(ResumeContractClauseWorkflowRequest request, StreamObserver<ResumeContractClauseWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeContractClauseWorkflow(request));
    }

    /**
     * Returns a contract clause.
     */
    @Override
    public void inspectContractClauseWorkflow(InspectContractClauseWorkflowRequest request, StreamObserver<InspectContractClauseWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectContractClauseWorkflow(request));
    }

    /**
     * Lists contract clauses a page at a time.
     */
    @Override
    public void listContractClauseWorkflows(ListContractClauseWorkflowsRequest request, StreamObserver<ListContractClauseWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listContractClauseWorkflows(request));
    }

    /**
     * Signals a contract clause.
     */
    @Override
    public void signalContractClauseWorkflow(SignalContractClauseWorkflowRequest request, StreamObserver<SignalContractClauseWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalContractClauseWorkflow(request));
    }
}
