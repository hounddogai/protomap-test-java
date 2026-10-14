package com.acme.finance.domains.contracts;

import com.acme.contracts.v1.CheckObligationEligibilityRequest;
import com.acme.contracts.v1.CheckObligationEligibilityResponse;
import com.acme.contracts.v1.ExplainObligationDecisionRequest;
import com.acme.contracts.v1.ExplainObligationDecisionResponse;
import com.acme.contracts.v1.GetObligationRulesRequest;
import com.acme.contracts.v1.GetObligationRulesResponse;
import com.acme.contracts.v1.LintObligationsRequest;
import com.acme.contracts.v1.LintObligationsResponse;
import com.acme.contracts.v1.NormalizeObligationRequest;
import com.acme.contracts.v1.NormalizeObligationResponse;
import com.acme.contracts.v1.ObligationValidationServiceGrpc;
import com.acme.contracts.v1.PreviewObligationRequest;
import com.acme.contracts.v1.PreviewObligationResponse;
import com.acme.contracts.v1.ScoreObligationRequest;
import com.acme.contracts.v1.ScoreObligationResponse;
import com.acme.contracts.v1.ValidateObligationRequest;
import com.acme.contracts.v1.ValidateObligationResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks obligations against the rules before anything acts on them.
 */
@GrpcService
public class ObligationValidationEndpoint extends ObligationValidationServiceGrpc.ObligationValidationServiceImplBase {
    protected final ObligationValidationApplication application;

    public ObligationValidationEndpoint(ObligationValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates an obligation.
     */
    @Override
    public void validateObligation(ValidateObligationRequest request, StreamObserver<ValidateObligationResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateObligation(request));
    }

    /**
     * Previews an obligation.
     */
    @Override
    public void previewObligation(PreviewObligationRequest request, StreamObserver<PreviewObligationResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewObligation(request));
    }

    /**
     * Checks an obligation.
     */
    @Override
    public void checkObligationEligibility(CheckObligationEligibilityRequest request, StreamObserver<CheckObligationEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkObligationEligibility(request));
    }

    /**
     * Normalizes an obligation.
     */
    @Override
    public void normalizeObligation(NormalizeObligationRequest request, StreamObserver<NormalizeObligationResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeObligation(request));
    }

    /**
     * Explains an obligation.
     */
    @Override
    public void explainObligationDecision(ExplainObligationDecisionRequest request, StreamObserver<ExplainObligationDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainObligationDecision(request));
    }

    /**
     * Scores an obligation.
     */
    @Override
    public void scoreObligation(ScoreObligationRequest request, StreamObserver<ScoreObligationResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreObligation(request));
    }

    /**
     * Lints an obligation.
     */
    @Override
    public void lintObligations(LintObligationsRequest request, StreamObserver<LintObligationsResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintObligations(request));
    }

    /**
     * Returns an obligation.
     */
    @Override
    public void getObligationRules(GetObligationRulesRequest request, StreamObserver<GetObligationRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getObligationRules(request));
    }
}
