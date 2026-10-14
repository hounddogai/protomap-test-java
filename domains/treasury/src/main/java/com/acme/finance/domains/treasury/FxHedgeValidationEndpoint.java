package com.acme.finance.domains.treasury;

import com.acme.finance.platform.RpcSupport;
import com.acme.treasury.v1.CheckFxHedgeEligibilityRequest;
import com.acme.treasury.v1.CheckFxHedgeEligibilityResponse;
import com.acme.treasury.v1.ExplainFxHedgeDecisionRequest;
import com.acme.treasury.v1.ExplainFxHedgeDecisionResponse;
import com.acme.treasury.v1.FxHedgeValidationServiceGrpc;
import com.acme.treasury.v1.GetFxHedgeRulesRequest;
import com.acme.treasury.v1.GetFxHedgeRulesResponse;
import com.acme.treasury.v1.LintFxHedgesRequest;
import com.acme.treasury.v1.LintFxHedgesResponse;
import com.acme.treasury.v1.NormalizeFxHedgeRequest;
import com.acme.treasury.v1.NormalizeFxHedgeResponse;
import com.acme.treasury.v1.PreviewFxHedgeRequest;
import com.acme.treasury.v1.PreviewFxHedgeResponse;
import com.acme.treasury.v1.ScoreFxHedgeRequest;
import com.acme.treasury.v1.ScoreFxHedgeResponse;
import com.acme.treasury.v1.ValidateFxHedgeRequest;
import com.acme.treasury.v1.ValidateFxHedgeResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks fx hedges against the rules before anything acts on them.
 */
@GrpcService
public class FxHedgeValidationEndpoint extends FxHedgeValidationServiceGrpc.FxHedgeValidationServiceImplBase {
    protected final FxHedgeValidationApplication application;

    public FxHedgeValidationEndpoint(FxHedgeValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a fx hedge.
     */
    @Override
    public void validateFxHedge(ValidateFxHedgeRequest request, StreamObserver<ValidateFxHedgeResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateFxHedge(request));
    }

    /**
     * Previews a fx hedge.
     */
    @Override
    public void previewFxHedge(PreviewFxHedgeRequest request, StreamObserver<PreviewFxHedgeResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewFxHedge(request));
    }

    /**
     * Checks a fx hedge.
     */
    @Override
    public void checkFxHedgeEligibility(CheckFxHedgeEligibilityRequest request, StreamObserver<CheckFxHedgeEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkFxHedgeEligibility(request));
    }

    /**
     * Normalizes a fx hedge.
     */
    @Override
    public void normalizeFxHedge(NormalizeFxHedgeRequest request, StreamObserver<NormalizeFxHedgeResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeFxHedge(request));
    }

    /**
     * Explains a fx hedge.
     */
    @Override
    public void explainFxHedgeDecision(ExplainFxHedgeDecisionRequest request, StreamObserver<ExplainFxHedgeDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainFxHedgeDecision(request));
    }

    /**
     * Scores a fx hedge.
     */
    @Override
    public void scoreFxHedge(ScoreFxHedgeRequest request, StreamObserver<ScoreFxHedgeResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreFxHedge(request));
    }

    /**
     * Lints a fx hedge.
     */
    @Override
    public void lintFxHedges(LintFxHedgesRequest request, StreamObserver<LintFxHedgesResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintFxHedges(request));
    }

    /**
     * Returns a fx hedge.
     */
    @Override
    public void getFxHedgeRules(GetFxHedgeRulesRequest request, StreamObserver<GetFxHedgeRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getFxHedgeRules(request));
    }
}
