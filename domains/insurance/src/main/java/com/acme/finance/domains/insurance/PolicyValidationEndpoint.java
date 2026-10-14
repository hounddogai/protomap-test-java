package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.CheckPolicyEligibilityRequest;
import com.acme.insurance.v1.CheckPolicyEligibilityResponse;
import com.acme.insurance.v1.ExplainPolicyDecisionRequest;
import com.acme.insurance.v1.ExplainPolicyDecisionResponse;
import com.acme.insurance.v1.GetPolicyRulesRequest;
import com.acme.insurance.v1.GetPolicyRulesResponse;
import com.acme.insurance.v1.LintPoliciesRequest;
import com.acme.insurance.v1.LintPoliciesResponse;
import com.acme.insurance.v1.NormalizePolicyRequest;
import com.acme.insurance.v1.NormalizePolicyResponse;
import com.acme.insurance.v1.PolicyValidationServiceGrpc;
import com.acme.insurance.v1.PreviewPolicyRequest;
import com.acme.insurance.v1.PreviewPolicyResponse;
import com.acme.insurance.v1.ValidatePolicyRequest;
import com.acme.insurance.v1.ValidatePolicyResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks policies against the rules before anything acts on them.
 */
@GrpcService
public class PolicyValidationEndpoint extends PolicyValidationServiceGrpc.PolicyValidationServiceImplBase {
    protected final PolicyValidationApplication application;

    public PolicyValidationEndpoint(PolicyValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a policy.
     */
    @Override
    public void validatePolicy(ValidatePolicyRequest request, StreamObserver<ValidatePolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.validatePolicy(request));
    }

    /**
     * Previews a policy.
     */
    @Override
    public void previewPolicy(PreviewPolicyRequest request, StreamObserver<PreviewPolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewPolicy(request));
    }

    /**
     * Checks a policy.
     */
    @Override
    public void checkPolicyEligibility(CheckPolicyEligibilityRequest request, StreamObserver<CheckPolicyEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkPolicyEligibility(request));
    }

    /**
     * Normalizes a policy.
     */
    @Override
    public void normalizePolicy(NormalizePolicyRequest request, StreamObserver<NormalizePolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizePolicy(request));
    }

    /**
     * Explains a policy.
     */
    @Override
    public void explainPolicyDecision(ExplainPolicyDecisionRequest request, StreamObserver<ExplainPolicyDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainPolicyDecision(request));
    }

    /**
     * Lints a policy.
     */
    @Override
    public void lintPolicies(LintPoliciesRequest request, StreamObserver<LintPoliciesResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintPolicies(request));
    }

    /**
     * Returns a policy.
     */
    @Override
    public void getPolicyRules(GetPolicyRulesRequest request, StreamObserver<GetPolicyRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getPolicyRules(request));
    }
}
