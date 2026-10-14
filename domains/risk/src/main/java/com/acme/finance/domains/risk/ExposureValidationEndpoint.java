package com.acme.finance.domains.risk;

import com.acme.finance.platform.RpcSupport;
import com.acme.risk.v1.CheckExposureEligibilityRequest;
import com.acme.risk.v1.CheckExposureEligibilityResponse;
import com.acme.risk.v1.ExplainExposureDecisionRequest;
import com.acme.risk.v1.ExplainExposureDecisionResponse;
import com.acme.risk.v1.ExposureValidationServiceGrpc;
import com.acme.risk.v1.GetExposureRulesRequest;
import com.acme.risk.v1.GetExposureRulesResponse;
import com.acme.risk.v1.LintExposuresRequest;
import com.acme.risk.v1.LintExposuresResponse;
import com.acme.risk.v1.NormalizeExposureRequest;
import com.acme.risk.v1.NormalizeExposureResponse;
import com.acme.risk.v1.PreviewExposureRequest;
import com.acme.risk.v1.PreviewExposureResponse;
import com.acme.risk.v1.ScoreExposureRequest;
import com.acme.risk.v1.ScoreExposureResponse;
import com.acme.risk.v1.ValidateExposureRequest;
import com.acme.risk.v1.ValidateExposureResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks exposures against the rules before anything acts on them.
 */
@GrpcService
public class ExposureValidationEndpoint extends ExposureValidationServiceGrpc.ExposureValidationServiceImplBase {
    protected final ExposureValidationApplication application;

    public ExposureValidationEndpoint(ExposureValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates an exposure.
     */
    @Override
    public void validateExposure(ValidateExposureRequest request, StreamObserver<ValidateExposureResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateExposure(request));
    }

    /**
     * Previews an exposure.
     */
    @Override
    public void previewExposure(PreviewExposureRequest request, StreamObserver<PreviewExposureResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewExposure(request));
    }

    /**
     * Checks an exposure.
     */
    @Override
    public void checkExposureEligibility(CheckExposureEligibilityRequest request, StreamObserver<CheckExposureEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkExposureEligibility(request));
    }

    /**
     * Normalizes an exposure.
     */
    @Override
    public void normalizeExposure(NormalizeExposureRequest request, StreamObserver<NormalizeExposureResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeExposure(request));
    }

    /**
     * Explains an exposure.
     */
    @Override
    public void explainExposureDecision(ExplainExposureDecisionRequest request, StreamObserver<ExplainExposureDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainExposureDecision(request));
    }

    /**
     * Scores an exposure.
     */
    @Override
    public void scoreExposure(ScoreExposureRequest request, StreamObserver<ScoreExposureResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreExposure(request));
    }

    /**
     * Lints an exposure.
     */
    @Override
    public void lintExposures(LintExposuresRequest request, StreamObserver<LintExposuresResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintExposures(request));
    }

    /**
     * Returns an exposure.
     */
    @Override
    public void getExposureRules(GetExposureRulesRequest request, StreamObserver<GetExposureRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getExposureRules(request));
    }
}
