package com.acme.finance.domains.billing;

import com.acme.billing.v1.CheckDunningEligibilityRequest;
import com.acme.billing.v1.CheckDunningEligibilityResponse;
import com.acme.billing.v1.DunningValidationServiceGrpc;
import com.acme.billing.v1.ExplainDunningDecisionRequest;
import com.acme.billing.v1.ExplainDunningDecisionResponse;
import com.acme.billing.v1.GetDunningRulesRequest;
import com.acme.billing.v1.GetDunningRulesResponse;
import com.acme.billing.v1.LintDunningsRequest;
import com.acme.billing.v1.LintDunningsResponse;
import com.acme.billing.v1.NormalizeDunningRequest;
import com.acme.billing.v1.NormalizeDunningResponse;
import com.acme.billing.v1.PreviewDunningRequest;
import com.acme.billing.v1.PreviewDunningResponse;
import com.acme.billing.v1.ScoreDunningRequest;
import com.acme.billing.v1.ScoreDunningResponse;
import com.acme.billing.v1.ValidateDunningRequest;
import com.acme.billing.v1.ValidateDunningResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks dunnings against the rules before anything acts on them.
 */
@GrpcService
public class DunningValidationEndpoint extends DunningValidationServiceGrpc.DunningValidationServiceImplBase {
    protected final DunningValidationApplication application;

    public DunningValidationEndpoint(DunningValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a dunning.
     */
    @Override
    public void validateDunning(ValidateDunningRequest request, StreamObserver<ValidateDunningResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateDunning(request));
    }

    /**
     * Previews a dunning.
     */
    @Override
    public void previewDunning(PreviewDunningRequest request, StreamObserver<PreviewDunningResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewDunning(request));
    }

    /**
     * Checks a dunning.
     */
    @Override
    public void checkDunningEligibility(CheckDunningEligibilityRequest request, StreamObserver<CheckDunningEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkDunningEligibility(request));
    }

    /**
     * Normalizes a dunning.
     */
    @Override
    public void normalizeDunning(NormalizeDunningRequest request, StreamObserver<NormalizeDunningResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeDunning(request));
    }

    /**
     * Explains a dunning.
     */
    @Override
    public void explainDunningDecision(ExplainDunningDecisionRequest request, StreamObserver<ExplainDunningDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainDunningDecision(request));
    }

    /**
     * Scores a dunning.
     */
    @Override
    public void scoreDunning(ScoreDunningRequest request, StreamObserver<ScoreDunningResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreDunning(request));
    }

    /**
     * Lints a dunning.
     */
    @Override
    public void lintDunnings(LintDunningsRequest request, StreamObserver<LintDunningsResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintDunnings(request));
    }

    /**
     * Returns a dunning.
     */
    @Override
    public void getDunningRules(GetDunningRulesRequest request, StreamObserver<GetDunningRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getDunningRules(request));
    }
}
