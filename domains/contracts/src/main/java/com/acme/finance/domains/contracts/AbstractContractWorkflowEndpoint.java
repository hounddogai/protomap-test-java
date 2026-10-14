package com.acme.finance.domains.contracts;

import com.acme.contracts.v1.AdvanceContractWorkflowRequest;
import com.acme.contracts.v1.AdvanceContractWorkflowResponse;
import com.acme.contracts.v1.CancelContractWorkflowRequest;
import com.acme.contracts.v1.CancelContractWorkflowResponse;
import com.acme.contracts.v1.ContractWorkflowServiceGrpc;
import com.acme.contracts.v1.InspectContractWorkflowRequest;
import com.acme.contracts.v1.InspectContractWorkflowResponse;
import com.acme.contracts.v1.ListContractWorkflowsRequest;
import com.acme.contracts.v1.ListContractWorkflowsResponse;
import com.acme.contracts.v1.PauseContractWorkflowRequest;
import com.acme.contracts.v1.PauseContractWorkflowResponse;
import com.acme.contracts.v1.ResumeContractWorkflowRequest;
import com.acme.contracts.v1.ResumeContractWorkflowResponse;
import com.acme.contracts.v1.RetryContractWorkflowRequest;
import com.acme.contracts.v1.RetryContractWorkflowResponse;
import com.acme.contracts.v1.SignalContractWorkflowRequest;
import com.acme.contracts.v1.SignalContractWorkflowResponse;
import com.acme.contracts.v1.StartContractWorkflowRequest;
import com.acme.contracts.v1.StartContractWorkflowResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Runs the long-lived workflow that takes a contract from start to finish.
 */
public abstract class AbstractContractWorkflowEndpoint extends ContractWorkflowServiceGrpc.ContractWorkflowServiceImplBase {
    protected final ContractWorkflowApplication application;

    protected AbstractContractWorkflowEndpoint(ContractWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a contract.
     */
    @Override
    public void startContractWorkflow(StartContractWorkflowRequest request, StreamObserver<StartContractWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startContractWorkflow(request));
    }

    /**
     * Advances a contract.
     */
    @Override
    public void advanceContractWorkflow(AdvanceContractWorkflowRequest request, StreamObserver<AdvanceContractWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advanceContractWorkflow(request));
    }

    /**
     * Cancels a contract.
     */
    @Override
    public void cancelContractWorkflow(CancelContractWorkflowRequest request, StreamObserver<CancelContractWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelContractWorkflow(request));
    }

    /**
     * Retrys a contract.
     */
    @Override
    public void retryContractWorkflow(RetryContractWorkflowRequest request, StreamObserver<RetryContractWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryContractWorkflow(request));
    }

    /**
     * Pauses a contract.
     */
    @Override
    public void pauseContractWorkflow(PauseContractWorkflowRequest request, StreamObserver<PauseContractWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pauseContractWorkflow(request));
    }

    /**
     * Resumes a contract.
     */
    @Override
    public void resumeContractWorkflow(ResumeContractWorkflowRequest request, StreamObserver<ResumeContractWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumeContractWorkflow(request));
    }

    /**
     * Returns a contract.
     */
    @Override
    public void inspectContractWorkflow(InspectContractWorkflowRequest request, StreamObserver<InspectContractWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectContractWorkflow(request));
    }

    /**
     * Lists contracts a page at a time.
     */
    @Override
    public void listContractWorkflows(ListContractWorkflowsRequest request, StreamObserver<ListContractWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listContractWorkflows(request));
    }

    /**
     * Signals a contract.
     */
    @Override
    public void signalContractWorkflow(SignalContractWorkflowRequest request, StreamObserver<SignalContractWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalContractWorkflow(request));
    }
}
