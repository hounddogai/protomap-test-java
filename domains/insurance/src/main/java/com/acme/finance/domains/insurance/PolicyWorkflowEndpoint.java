package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.AdvancePolicyWorkflowRequest;
import com.acme.insurance.v1.AdvancePolicyWorkflowResponse;
import com.acme.insurance.v1.CancelPolicyWorkflowRequest;
import com.acme.insurance.v1.CancelPolicyWorkflowResponse;
import com.acme.insurance.v1.ListPolicyWorkflowsRequest;
import com.acme.insurance.v1.ListPolicyWorkflowsResponse;
import com.acme.insurance.v1.PausePolicyWorkflowRequest;
import com.acme.insurance.v1.PausePolicyWorkflowResponse;
import com.acme.insurance.v1.PolicyWorkflowServiceGrpc;
import com.acme.insurance.v1.ResumePolicyWorkflowRequest;
import com.acme.insurance.v1.ResumePolicyWorkflowResponse;
import com.acme.insurance.v1.SignalPolicyWorkflowRequest;
import com.acme.insurance.v1.SignalPolicyWorkflowResponse;
import com.acme.insurance.v1.StartPolicyWorkflowRequest;
import com.acme.insurance.v1.StartPolicyWorkflowResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Runs the long-lived workflow that takes a policy from start to finish.
 */
@GrpcService
public class PolicyWorkflowEndpoint extends PolicyWorkflowServiceGrpc.PolicyWorkflowServiceImplBase {
    protected final PolicyWorkflowApplication application;

    public PolicyWorkflowEndpoint(PolicyWorkflowApplication application) {
        this.application = application;
    }

    /**
     * Starts a policy.
     */
    @Override
    public void startPolicyWorkflow(StartPolicyWorkflowRequest request, StreamObserver<StartPolicyWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.startPolicyWorkflow(request));
    }

    /**
     * Advances a policy.
     */
    @Override
    public void advancePolicyWorkflow(AdvancePolicyWorkflowRequest request, StreamObserver<AdvancePolicyWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.advancePolicyWorkflow(request));
    }

    /**
     * Cancels a policy.
     */
    @Override
    public void cancelPolicyWorkflow(CancelPolicyWorkflowRequest request, StreamObserver<CancelPolicyWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelPolicyWorkflow(request));
    }

    /**
     * Pauses a policy.
     */
    @Override
    public void pausePolicyWorkflow(PausePolicyWorkflowRequest request, StreamObserver<PausePolicyWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.pausePolicyWorkflow(request));
    }

    /**
     * Resumes a policy.
     */
    @Override
    public void resumePolicyWorkflow(ResumePolicyWorkflowRequest request, StreamObserver<ResumePolicyWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.resumePolicyWorkflow(request));
    }

    /**
     * Lists policies a page at a time.
     */
    @Override
    public void listPolicyWorkflows(ListPolicyWorkflowsRequest request, StreamObserver<ListPolicyWorkflowsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPolicyWorkflows(request));
    }

    /**
     * Signals a policy.
     */
    @Override
    public void signalPolicyWorkflow(SignalPolicyWorkflowRequest request, StreamObserver<SignalPolicyWorkflowResponse> observer) {
        RpcSupport.unary(observer, () -> application.signalPolicyWorkflow(request));
    }
}
