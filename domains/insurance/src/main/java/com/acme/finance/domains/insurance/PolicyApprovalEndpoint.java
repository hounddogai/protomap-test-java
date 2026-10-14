package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.ApprovePolicyRequest;
import com.acme.insurance.v1.ApprovePolicyResponse;
import com.acme.insurance.v1.CommentPolicyApprovalRequest;
import com.acme.insurance.v1.CommentPolicyApprovalResponse;
import com.acme.insurance.v1.EscalatePolicyRequest;
import com.acme.insurance.v1.EscalatePolicyResponse;
import com.acme.insurance.v1.GetPolicyApprovalStatusRequest;
import com.acme.insurance.v1.GetPolicyApprovalStatusResponse;
import com.acme.insurance.v1.ListPendingPoliciesRequest;
import com.acme.insurance.v1.ListPendingPoliciesResponse;
import com.acme.insurance.v1.PolicyApprovalServiceGrpc;
import com.acme.insurance.v1.RecallPolicyRequest;
import com.acme.insurance.v1.RecallPolicyResponse;
import com.acme.insurance.v1.RejectPolicyRequest;
import com.acme.insurance.v1.RejectPolicyResponse;
import com.acme.insurance.v1.SubmitPolicyRequest;
import com.acme.insurance.v1.SubmitPolicyResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes policies through approval, with escalation and delegation.
 */
@GrpcService
public class PolicyApprovalEndpoint extends PolicyApprovalServiceGrpc.PolicyApprovalServiceImplBase {
    protected final PolicyApprovalApplication application;

    public PolicyApprovalEndpoint(PolicyApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a policy.
     */
    @Override
    public void submitPolicy(SubmitPolicyRequest request, StreamObserver<SubmitPolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitPolicy(request));
    }

    /**
     * Approves a policy.
     */
    @Override
    public void approvePolicy(ApprovePolicyRequest request, StreamObserver<ApprovePolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.approvePolicy(request));
    }

    /**
     * Rejects a policy.
     */
    @Override
    public void rejectPolicy(RejectPolicyRequest request, StreamObserver<RejectPolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectPolicy(request));
    }

    /**
     * Escalates a policy.
     */
    @Override
    public void escalatePolicy(EscalatePolicyRequest request, StreamObserver<EscalatePolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalatePolicy(request));
    }

    /**
     * Lists policies a page at a time.
     */
    @Override
    public void listPendingPolicies(ListPendingPoliciesRequest request, StreamObserver<ListPendingPoliciesResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingPolicies(request));
    }

    /**
     * Recalls a policy.
     */
    @Override
    public void recallPolicy(RecallPolicyRequest request, StreamObserver<RecallPolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallPolicy(request));
    }

    /**
     * Returns a policy.
     */
    @Override
    public void getPolicyApprovalStatus(GetPolicyApprovalStatusRequest request, StreamObserver<GetPolicyApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getPolicyApprovalStatus(request));
    }

    /**
     * Comments a policy.
     */
    @Override
    public void commentPolicyApproval(CommentPolicyApprovalRequest request, StreamObserver<CommentPolicyApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentPolicyApproval(request));
    }
}
