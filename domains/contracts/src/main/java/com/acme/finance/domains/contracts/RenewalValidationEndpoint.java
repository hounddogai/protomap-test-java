package com.acme.finance.domains.contracts;

import com.acme.contracts.v1.CheckRenewalEligibilityRequest;
import com.acme.contracts.v1.CheckRenewalEligibilityResponse;
import com.acme.contracts.v1.ExplainRenewalDecisionRequest;
import com.acme.contracts.v1.ExplainRenewalDecisionResponse;
import com.acme.contracts.v1.GetRenewalRulesRequest;
import com.acme.contracts.v1.GetRenewalRulesResponse;
import com.acme.contracts.v1.NormalizeRenewalRequest;
import com.acme.contracts.v1.NormalizeRenewalResponse;
import com.acme.contracts.v1.PreviewRenewalRequest;
import com.acme.contracts.v1.PreviewRenewalResponse;
import com.acme.contracts.v1.RenewalValidationServiceGrpc;
import com.acme.contracts.v1.ScoreRenewalRequest;
import com.acme.contracts.v1.ScoreRenewalResponse;
import com.acme.contracts.v1.ValidateRenewalRequest;
import com.acme.contracts.v1.ValidateRenewalResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks renewals against the rules before anything acts on them.
 */
@GrpcService
public class RenewalValidationEndpoint extends RenewalValidationServiceGrpc.RenewalValidationServiceImplBase {
    protected final RenewalValidationApplication application;

    public RenewalValidationEndpoint(RenewalValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a renewal.
     */
    @Override
    public void validateRenewal(ValidateRenewalRequest request, StreamObserver<ValidateRenewalResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateRenewal(request));
    }

    /**
     * Previews a renewal.
     */
    @Override
    public void previewRenewal(PreviewRenewalRequest request, StreamObserver<PreviewRenewalResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewRenewal(request));
    }

    /**
     * Checks a renewal.
     */
    @Override
    public void checkRenewalEligibility(CheckRenewalEligibilityRequest request, StreamObserver<CheckRenewalEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkRenewalEligibility(request));
    }

    /**
     * Normalizes a renewal.
     */
    @Override
    public void normalizeRenewal(NormalizeRenewalRequest request, StreamObserver<NormalizeRenewalResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeRenewal(request));
    }

    /**
     * Explains a renewal.
     */
    @Override
    public void explainRenewalDecision(ExplainRenewalDecisionRequest request, StreamObserver<ExplainRenewalDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainRenewalDecision(request));
    }

    /**
     * Scores a renewal.
     */
    @Override
    public void scoreRenewal(ScoreRenewalRequest request, StreamObserver<ScoreRenewalResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreRenewal(request));
    }

    /**
     * Returns a renewal.
     */
    @Override
    public void getRenewalRules(GetRenewalRulesRequest request, StreamObserver<GetRenewalRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getRenewalRules(request));
    }
}
