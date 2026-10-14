package com.acme.finance.domains.audit;

import com.acme.audit.v1.CheckRetentionPolicyEligibilityRequest;
import com.acme.audit.v1.CheckRetentionPolicyEligibilityResponse;
import com.acme.audit.v1.ExplainRetentionPolicyDecisionRequest;
import com.acme.audit.v1.ExplainRetentionPolicyDecisionResponse;
import com.acme.audit.v1.GetRetentionPolicyRulesRequest;
import com.acme.audit.v1.GetRetentionPolicyRulesResponse;
import com.acme.audit.v1.LintRetentionPoliciesRequest;
import com.acme.audit.v1.LintRetentionPoliciesResponse;
import com.acme.audit.v1.NormalizeRetentionPolicyRequest;
import com.acme.audit.v1.NormalizeRetentionPolicyResponse;
import com.acme.audit.v1.PreviewRetentionPolicyRequest;
import com.acme.audit.v1.PreviewRetentionPolicyResponse;
import com.acme.audit.v1.RetentionPolicyValidationServiceGrpc;
import com.acme.audit.v1.ScoreRetentionPolicyRequest;
import com.acme.audit.v1.ScoreRetentionPolicyResponse;
import com.acme.audit.v1.ValidateRetentionPolicyRequest;
import com.acme.audit.v1.ValidateRetentionPolicyResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks retention policies against the rules before anything acts on them.
 */
@GrpcService
public class RetentionPolicyValidationEndpoint extends RetentionPolicyValidationServiceGrpc.RetentionPolicyValidationServiceImplBase {
    protected final RetentionPolicyValidationApplication application;

    public RetentionPolicyValidationEndpoint(RetentionPolicyValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a retention policy.
     */
    @Override
    public void validateRetentionPolicy(ValidateRetentionPolicyRequest request, StreamObserver<ValidateRetentionPolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateRetentionPolicy(request));
    }

    /**
     * Previews a retention policy.
     */
    @Override
    public void previewRetentionPolicy(PreviewRetentionPolicyRequest request, StreamObserver<PreviewRetentionPolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewRetentionPolicy(request));
    }

    /**
     * Checks a retention policy.
     */
    @Override
    public void checkRetentionPolicyEligibility(CheckRetentionPolicyEligibilityRequest request, StreamObserver<CheckRetentionPolicyEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkRetentionPolicyEligibility(request));
    }

    /**
     * Normalizes a retention policy.
     */
    @Override
    public void normalizeRetentionPolicy(NormalizeRetentionPolicyRequest request, StreamObserver<NormalizeRetentionPolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeRetentionPolicy(request));
    }

    /**
     * Explains a retention policy.
     */
    @Override
    public void explainRetentionPolicyDecision(ExplainRetentionPolicyDecisionRequest request, StreamObserver<ExplainRetentionPolicyDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainRetentionPolicyDecision(request));
    }

    /**
     * Scores a retention policy.
     */
    @Override
    public void scoreRetentionPolicy(ScoreRetentionPolicyRequest request, StreamObserver<ScoreRetentionPolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreRetentionPolicy(request));
    }

    /**
     * Lints a retention policy.
     */
    @Override
    public void lintRetentionPolicies(LintRetentionPoliciesRequest request, StreamObserver<LintRetentionPoliciesResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintRetentionPolicies(request));
    }

    /**
     * Returns a retention policy.
     */
    @Override
    public void getRetentionPolicyRules(GetRetentionPolicyRulesRequest request, StreamObserver<GetRetentionPolicyRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getRetentionPolicyRules(request));
    }
}
