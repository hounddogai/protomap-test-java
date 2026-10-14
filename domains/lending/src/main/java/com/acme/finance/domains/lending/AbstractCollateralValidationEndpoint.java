package com.acme.finance.domains.lending;

import com.acme.finance.platform.RpcSupport;
import com.acme.lending.v1.CheckCollateralEligibilityRequest;
import com.acme.lending.v1.CheckCollateralEligibilityResponse;
import com.acme.lending.v1.CollateralValidationServiceGrpc;
import com.acme.lending.v1.ExplainCollateralDecisionRequest;
import com.acme.lending.v1.ExplainCollateralDecisionResponse;
import com.acme.lending.v1.GetCollateralRulesRequest;
import com.acme.lending.v1.GetCollateralRulesResponse;
import com.acme.lending.v1.LintCollateralsRequest;
import com.acme.lending.v1.LintCollateralsResponse;
import com.acme.lending.v1.NormalizeCollateralRequest;
import com.acme.lending.v1.NormalizeCollateralResponse;
import com.acme.lending.v1.PreviewCollateralRequest;
import com.acme.lending.v1.PreviewCollateralResponse;
import com.acme.lending.v1.ScoreCollateralRequest;
import com.acme.lending.v1.ScoreCollateralResponse;
import com.acme.lending.v1.ValidateCollateralRequest;
import com.acme.lending.v1.ValidateCollateralResponse;
import io.grpc.stub.StreamObserver;

/**
 * Checks collaterals against the rules before anything acts on them.
 */
public abstract class AbstractCollateralValidationEndpoint extends CollateralValidationServiceGrpc.CollateralValidationServiceImplBase {
    protected final CollateralValidationApplication application;

    protected AbstractCollateralValidationEndpoint(CollateralValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a collateral.
     */
    @Override
    public void validateCollateral(ValidateCollateralRequest request, StreamObserver<ValidateCollateralResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateCollateral(request));
    }

    /**
     * Previews a collateral.
     */
    @Override
    public void previewCollateral(PreviewCollateralRequest request, StreamObserver<PreviewCollateralResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewCollateral(request));
    }

    /**
     * Checks a collateral.
     */
    @Override
    public void checkCollateralEligibility(CheckCollateralEligibilityRequest request, StreamObserver<CheckCollateralEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkCollateralEligibility(request));
    }

    /**
     * Normalizes a collateral.
     */
    @Override
    public void normalizeCollateral(NormalizeCollateralRequest request, StreamObserver<NormalizeCollateralResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeCollateral(request));
    }

    /**
     * Explains a collateral.
     */
    @Override
    public void explainCollateralDecision(ExplainCollateralDecisionRequest request, StreamObserver<ExplainCollateralDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainCollateralDecision(request));
    }

    /**
     * Scores a collateral.
     */
    @Override
    public void scoreCollateral(ScoreCollateralRequest request, StreamObserver<ScoreCollateralResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreCollateral(request));
    }

    /**
     * Lints a collateral.
     */
    @Override
    public void lintCollaterals(LintCollateralsRequest request, StreamObserver<LintCollateralsResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintCollaterals(request));
    }

    /**
     * Returns a collateral.
     */
    @Override
    public void getCollateralRules(GetCollateralRulesRequest request, StreamObserver<GetCollateralRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getCollateralRules(request));
    }
}
