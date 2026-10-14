package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.CheckPremiumEligibilityRequest;
import com.acme.insurance.v1.CheckPremiumEligibilityResponse;
import com.acme.insurance.v1.GetPremiumRulesRequest;
import com.acme.insurance.v1.GetPremiumRulesResponse;
import com.acme.insurance.v1.LintPremiumsRequest;
import com.acme.insurance.v1.LintPremiumsResponse;
import com.acme.insurance.v1.NormalizePremiumRequest;
import com.acme.insurance.v1.NormalizePremiumResponse;
import com.acme.insurance.v1.PremiumValidationServiceGrpc;
import com.acme.insurance.v1.PreviewPremiumRequest;
import com.acme.insurance.v1.PreviewPremiumResponse;
import com.acme.insurance.v1.ScorePremiumRequest;
import com.acme.insurance.v1.ScorePremiumResponse;
import com.acme.insurance.v1.ValidatePremiumRequest;
import com.acme.insurance.v1.ValidatePremiumResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks premiums against the rules before anything acts on them.
 */
@GrpcService
public class PremiumValidationEndpoint extends PremiumValidationServiceGrpc.PremiumValidationServiceImplBase {
    protected final PremiumValidationApplication application;

    public PremiumValidationEndpoint(PremiumValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a premium.
     */
    @Override
    public void validatePremium(ValidatePremiumRequest request, StreamObserver<ValidatePremiumResponse> observer) {
        RpcSupport.unary(observer, () -> application.validatePremium(request));
    }

    /**
     * Previews a premium.
     */
    @Override
    public void previewPremium(PreviewPremiumRequest request, StreamObserver<PreviewPremiumResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewPremium(request));
    }

    /**
     * Checks a premium.
     */
    @Override
    public void checkPremiumEligibility(CheckPremiumEligibilityRequest request, StreamObserver<CheckPremiumEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkPremiumEligibility(request));
    }

    /**
     * Normalizes a premium.
     */
    @Override
    public void normalizePremium(NormalizePremiumRequest request, StreamObserver<NormalizePremiumResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizePremium(request));
    }

    /**
     * Scores a premium.
     */
    @Override
    public void scorePremium(ScorePremiumRequest request, StreamObserver<ScorePremiumResponse> observer) {
        RpcSupport.unary(observer, () -> application.scorePremium(request));
    }

    /**
     * Lints a premium.
     */
    @Override
    public void lintPremiums(LintPremiumsRequest request, StreamObserver<LintPremiumsResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintPremiums(request));
    }

    /**
     * Returns a premium.
     */
    @Override
    public void getPremiumRules(GetPremiumRulesRequest request, StreamObserver<GetPremiumRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getPremiumRules(request));
    }
}
