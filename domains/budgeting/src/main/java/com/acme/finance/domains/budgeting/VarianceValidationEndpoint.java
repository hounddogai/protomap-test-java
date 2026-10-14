package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.CheckVarianceEligibilityRequest;
import com.acme.budgeting.v1.CheckVarianceEligibilityResponse;
import com.acme.budgeting.v1.ExplainVarianceDecisionRequest;
import com.acme.budgeting.v1.ExplainVarianceDecisionResponse;
import com.acme.budgeting.v1.GetVarianceRulesRequest;
import com.acme.budgeting.v1.GetVarianceRulesResponse;
import com.acme.budgeting.v1.PreviewVarianceRequest;
import com.acme.budgeting.v1.PreviewVarianceResponse;
import com.acme.budgeting.v1.ScoreVarianceRequest;
import com.acme.budgeting.v1.ScoreVarianceResponse;
import com.acme.budgeting.v1.ValidateVarianceRequest;
import com.acme.budgeting.v1.ValidateVarianceResponse;
import com.acme.budgeting.v1.VarianceValidationServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks variances against the rules before anything acts on them.
 */
@GrpcService
public class VarianceValidationEndpoint extends VarianceValidationServiceGrpc.VarianceValidationServiceImplBase {
    protected final VarianceValidationApplication application;

    public VarianceValidationEndpoint(VarianceValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a variance.
     */
    @Override
    public void validateVariance(ValidateVarianceRequest request, StreamObserver<ValidateVarianceResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateVariance(request));
    }

    /**
     * Previews a variance.
     */
    @Override
    public void previewVariance(PreviewVarianceRequest request, StreamObserver<PreviewVarianceResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewVariance(request));
    }

    /**
     * Checks a variance.
     */
    @Override
    public void checkVarianceEligibility(CheckVarianceEligibilityRequest request, StreamObserver<CheckVarianceEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkVarianceEligibility(request));
    }

    /**
     * Explains a variance.
     */
    @Override
    public void explainVarianceDecision(ExplainVarianceDecisionRequest request, StreamObserver<ExplainVarianceDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainVarianceDecision(request));
    }

    /**
     * Scores a variance.
     */
    @Override
    public void scoreVariance(ScoreVarianceRequest request, StreamObserver<ScoreVarianceResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreVariance(request));
    }

    /**
     * Returns a variance.
     */
    @Override
    public void getVarianceRules(GetVarianceRulesRequest request, StreamObserver<GetVarianceRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getVarianceRules(request));
    }
}
