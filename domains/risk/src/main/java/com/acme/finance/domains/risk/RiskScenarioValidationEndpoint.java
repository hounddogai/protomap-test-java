package com.acme.finance.domains.risk;

import com.acme.finance.platform.RpcSupport;
import com.acme.risk.v1.CheckRiskScenarioEligibilityRequest;
import com.acme.risk.v1.CheckRiskScenarioEligibilityResponse;
import com.acme.risk.v1.ExplainRiskScenarioDecisionRequest;
import com.acme.risk.v1.ExplainRiskScenarioDecisionResponse;
import com.acme.risk.v1.GetRiskScenarioRulesRequest;
import com.acme.risk.v1.GetRiskScenarioRulesResponse;
import com.acme.risk.v1.LintRiskScenariosRequest;
import com.acme.risk.v1.LintRiskScenariosResponse;
import com.acme.risk.v1.NormalizeRiskScenarioRequest;
import com.acme.risk.v1.NormalizeRiskScenarioResponse;
import com.acme.risk.v1.PreviewRiskScenarioRequest;
import com.acme.risk.v1.PreviewRiskScenarioResponse;
import com.acme.risk.v1.RiskScenarioValidationServiceGrpc;
import com.acme.risk.v1.ScoreRiskScenarioRequest;
import com.acme.risk.v1.ScoreRiskScenarioResponse;
import com.acme.risk.v1.ValidateRiskScenarioRequest;
import com.acme.risk.v1.ValidateRiskScenarioResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks risk scenarios against the rules before anything acts on them.
 */
@GrpcService
public class RiskScenarioValidationEndpoint extends RiskScenarioValidationServiceGrpc.RiskScenarioValidationServiceImplBase {
    protected final RiskScenarioValidationApplication application;

    public RiskScenarioValidationEndpoint(RiskScenarioValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a risk scenario.
     */
    @Override
    public void validateRiskScenario(ValidateRiskScenarioRequest request, StreamObserver<ValidateRiskScenarioResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateRiskScenario(request));
    }

    /**
     * Previews a risk scenario.
     */
    @Override
    public void previewRiskScenario(PreviewRiskScenarioRequest request, StreamObserver<PreviewRiskScenarioResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewRiskScenario(request));
    }

    /**
     * Checks a risk scenario.
     */
    @Override
    public void checkRiskScenarioEligibility(CheckRiskScenarioEligibilityRequest request, StreamObserver<CheckRiskScenarioEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkRiskScenarioEligibility(request));
    }

    /**
     * Normalizes a risk scenario.
     */
    @Override
    public void normalizeRiskScenario(NormalizeRiskScenarioRequest request, StreamObserver<NormalizeRiskScenarioResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeRiskScenario(request));
    }

    /**
     * Explains a risk scenario.
     */
    @Override
    public void explainRiskScenarioDecision(ExplainRiskScenarioDecisionRequest request, StreamObserver<ExplainRiskScenarioDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainRiskScenarioDecision(request));
    }

    /**
     * Scores a risk scenario.
     */
    @Override
    public void scoreRiskScenario(ScoreRiskScenarioRequest request, StreamObserver<ScoreRiskScenarioResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreRiskScenario(request));
    }

    /**
     * Lints a risk scenario.
     */
    @Override
    public void lintRiskScenarios(LintRiskScenariosRequest request, StreamObserver<LintRiskScenariosResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintRiskScenarios(request));
    }

    /**
     * Returns a risk scenario.
     */
    @Override
    public void getRiskScenarioRules(GetRiskScenarioRulesRequest request, StreamObserver<GetRiskScenarioRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getRiskScenarioRules(request));
    }
}
