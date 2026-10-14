package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.CheckClaimEligibilityRequest;
import com.acme.insurance.v1.CheckClaimEligibilityResponse;
import com.acme.insurance.v1.ClaimValidationServiceGrpc;
import com.acme.insurance.v1.ExplainClaimDecisionRequest;
import com.acme.insurance.v1.ExplainClaimDecisionResponse;
import com.acme.insurance.v1.GetClaimRulesRequest;
import com.acme.insurance.v1.GetClaimRulesResponse;
import com.acme.insurance.v1.LintClaimsRequest;
import com.acme.insurance.v1.LintClaimsResponse;
import com.acme.insurance.v1.NormalizeClaimRequest;
import com.acme.insurance.v1.NormalizeClaimResponse;
import com.acme.insurance.v1.PreviewClaimRequest;
import com.acme.insurance.v1.PreviewClaimResponse;
import com.acme.insurance.v1.ScoreClaimRequest;
import com.acme.insurance.v1.ScoreClaimResponse;
import com.acme.insurance.v1.ValidateClaimRequest;
import com.acme.insurance.v1.ValidateClaimResponse;
import io.grpc.stub.StreamObserver;

/**
 * Checks claims against the rules before anything acts on them.
 */
public abstract class AbstractClaimValidationEndpoint extends ClaimValidationServiceGrpc.ClaimValidationServiceImplBase {
    protected final ClaimValidationApplication application;

    protected AbstractClaimValidationEndpoint(ClaimValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a claim.
     */
    @Override
    public void validateClaim(ValidateClaimRequest request, StreamObserver<ValidateClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateClaim(request));
    }

    /**
     * Previews a claim.
     */
    @Override
    public void previewClaim(PreviewClaimRequest request, StreamObserver<PreviewClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewClaim(request));
    }

    /**
     * Checks a claim.
     */
    @Override
    public void checkClaimEligibility(CheckClaimEligibilityRequest request, StreamObserver<CheckClaimEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkClaimEligibility(request));
    }

    /**
     * Normalizes a claim.
     */
    @Override
    public void normalizeClaim(NormalizeClaimRequest request, StreamObserver<NormalizeClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeClaim(request));
    }

    /**
     * Explains a claim.
     */
    @Override
    public void explainClaimDecision(ExplainClaimDecisionRequest request, StreamObserver<ExplainClaimDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainClaimDecision(request));
    }

    /**
     * Scores a claim.
     */
    @Override
    public void scoreClaim(ScoreClaimRequest request, StreamObserver<ScoreClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreClaim(request));
    }

    /**
     * Lints a claim.
     */
    @Override
    public void lintClaims(LintClaimsRequest request, StreamObserver<LintClaimsResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintClaims(request));
    }

    /**
     * Returns a claim.
     */
    @Override
    public void getClaimRules(GetClaimRulesRequest request, StreamObserver<GetClaimRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getClaimRules(request));
    }
}
