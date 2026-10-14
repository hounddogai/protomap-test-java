package com.acme.finance.domains.assets;

import com.acme.assets.v1.CheckDepreciationEligibilityRequest;
import com.acme.assets.v1.CheckDepreciationEligibilityResponse;
import com.acme.assets.v1.DepreciationValidationServiceGrpc;
import com.acme.assets.v1.ExplainDepreciationDecisionRequest;
import com.acme.assets.v1.ExplainDepreciationDecisionResponse;
import com.acme.assets.v1.GetDepreciationRulesRequest;
import com.acme.assets.v1.GetDepreciationRulesResponse;
import com.acme.assets.v1.LintDepreciationsRequest;
import com.acme.assets.v1.LintDepreciationsResponse;
import com.acme.assets.v1.NormalizeDepreciationRequest;
import com.acme.assets.v1.NormalizeDepreciationResponse;
import com.acme.assets.v1.PreviewDepreciationRequest;
import com.acme.assets.v1.PreviewDepreciationResponse;
import com.acme.assets.v1.ScoreDepreciationRequest;
import com.acme.assets.v1.ScoreDepreciationResponse;
import com.acme.assets.v1.ValidateDepreciationRequest;
import com.acme.assets.v1.ValidateDepreciationResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks depreciations against the rules before anything acts on them.
 */
@GrpcService
public class DepreciationValidationEndpoint extends DepreciationValidationServiceGrpc.DepreciationValidationServiceImplBase {
    protected final DepreciationValidationApplication application;

    public DepreciationValidationEndpoint(DepreciationValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a depreciation.
     */
    @Override
    public void validateDepreciation(ValidateDepreciationRequest request, StreamObserver<ValidateDepreciationResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateDepreciation(request));
    }

    /**
     * Previews a depreciation.
     */
    @Override
    public void previewDepreciation(PreviewDepreciationRequest request, StreamObserver<PreviewDepreciationResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewDepreciation(request));
    }

    /**
     * Checks a depreciation.
     */
    @Override
    public void checkDepreciationEligibility(CheckDepreciationEligibilityRequest request, StreamObserver<CheckDepreciationEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkDepreciationEligibility(request));
    }

    /**
     * Normalizes a depreciation.
     */
    @Override
    public void normalizeDepreciation(NormalizeDepreciationRequest request, StreamObserver<NormalizeDepreciationResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeDepreciation(request));
    }

    /**
     * Explains a depreciation.
     */
    @Override
    public void explainDepreciationDecision(ExplainDepreciationDecisionRequest request, StreamObserver<ExplainDepreciationDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainDepreciationDecision(request));
    }

    /**
     * Scores a depreciation.
     */
    @Override
    public void scoreDepreciation(ScoreDepreciationRequest request, StreamObserver<ScoreDepreciationResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreDepreciation(request));
    }

    /**
     * Lints a depreciation.
     */
    @Override
    public void lintDepreciations(LintDepreciationsRequest request, StreamObserver<LintDepreciationsResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintDepreciations(request));
    }

    /**
     * Returns a depreciation.
     */
    @Override
    public void getDepreciationRules(GetDepreciationRulesRequest request, StreamObserver<GetDepreciationRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getDepreciationRules(request));
    }
}
