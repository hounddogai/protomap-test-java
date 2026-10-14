package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.CheckCoverageEligibilityRequest;
import com.acme.insurance.v1.CheckCoverageEligibilityResponse;
import com.acme.insurance.v1.CoverageValidationServiceGrpc;
import com.acme.insurance.v1.ExplainCoverageDecisionRequest;
import com.acme.insurance.v1.ExplainCoverageDecisionResponse;
import com.acme.insurance.v1.GetCoverageRulesRequest;
import com.acme.insurance.v1.GetCoverageRulesResponse;
import com.acme.insurance.v1.NormalizeCoverageRequest;
import com.acme.insurance.v1.NormalizeCoverageResponse;
import com.acme.insurance.v1.PreviewCoverageRequest;
import com.acme.insurance.v1.PreviewCoverageResponse;
import com.acme.insurance.v1.ScoreCoverageRequest;
import com.acme.insurance.v1.ScoreCoverageResponse;
import com.acme.insurance.v1.ValidateCoverageRequest;
import com.acme.insurance.v1.ValidateCoverageResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks coverages against the rules before anything acts on them.
 */
@GrpcService
public class CoverageValidationEndpoint extends CoverageValidationServiceGrpc.CoverageValidationServiceImplBase {
    protected final CoverageValidationApplication application;

    public CoverageValidationEndpoint(CoverageValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a coverage.
     */
    @Override
    public void validateCoverage(ValidateCoverageRequest request, StreamObserver<ValidateCoverageResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateCoverage(request));
    }

    /**
     * Previews a coverage.
     */
    @Override
    public void previewCoverage(PreviewCoverageRequest request, StreamObserver<PreviewCoverageResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewCoverage(request));
    }

    /**
     * Checks a coverage.
     */
    @Override
    public void checkCoverageEligibility(CheckCoverageEligibilityRequest request, StreamObserver<CheckCoverageEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkCoverageEligibility(request));
    }

    /**
     * Normalizes a coverage.
     */
    @Override
    public void normalizeCoverage(NormalizeCoverageRequest request, StreamObserver<NormalizeCoverageResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeCoverage(request));
    }

    /**
     * Explains a coverage.
     */
    @Override
    public void explainCoverageDecision(ExplainCoverageDecisionRequest request, StreamObserver<ExplainCoverageDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainCoverageDecision(request));
    }

    /**
     * Scores a coverage.
     */
    @Override
    public void scoreCoverage(ScoreCoverageRequest request, StreamObserver<ScoreCoverageResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreCoverage(request));
    }

    /**
     * Returns a coverage.
     */
    @Override
    public void getCoverageRules(GetCoverageRulesRequest request, StreamObserver<GetCoverageRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getCoverageRules(request));
    }
}
