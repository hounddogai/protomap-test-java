package com.acme.finance.domains.risk;

import com.acme.finance.platform.RpcSupport;
import com.acme.risk.v1.CheckRiskRegisterEligibilityRequest;
import com.acme.risk.v1.CheckRiskRegisterEligibilityResponse;
import com.acme.risk.v1.ExplainRiskRegisterDecisionRequest;
import com.acme.risk.v1.ExplainRiskRegisterDecisionResponse;
import com.acme.risk.v1.GetRiskRegisterRulesRequest;
import com.acme.risk.v1.GetRiskRegisterRulesResponse;
import com.acme.risk.v1.LintRiskRegistersRequest;
import com.acme.risk.v1.LintRiskRegistersResponse;
import com.acme.risk.v1.NormalizeRiskRegisterRequest;
import com.acme.risk.v1.NormalizeRiskRegisterResponse;
import com.acme.risk.v1.PreviewRiskRegisterRequest;
import com.acme.risk.v1.PreviewRiskRegisterResponse;
import com.acme.risk.v1.RiskRegisterValidationServiceGrpc;
import com.acme.risk.v1.ScoreRiskRegisterRequest;
import com.acme.risk.v1.ScoreRiskRegisterResponse;
import com.acme.risk.v1.ValidateRiskRegisterRequest;
import com.acme.risk.v1.ValidateRiskRegisterResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks risk registers against the rules before anything acts on them.
 */
@GrpcService
public class RiskRegisterValidationEndpoint extends RiskRegisterValidationServiceGrpc.RiskRegisterValidationServiceImplBase {
    protected final RiskRegisterValidationApplication application;

    public RiskRegisterValidationEndpoint(RiskRegisterValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a risk register.
     */
    @Override
    public void validateRiskRegister(ValidateRiskRegisterRequest request, StreamObserver<ValidateRiskRegisterResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateRiskRegister(request));
    }

    /**
     * Previews a risk register.
     */
    @Override
    public void previewRiskRegister(PreviewRiskRegisterRequest request, StreamObserver<PreviewRiskRegisterResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewRiskRegister(request));
    }

    /**
     * Checks a risk register.
     */
    @Override
    public void checkRiskRegisterEligibility(CheckRiskRegisterEligibilityRequest request, StreamObserver<CheckRiskRegisterEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkRiskRegisterEligibility(request));
    }

    /**
     * Normalizes a risk register.
     */
    @Override
    public void normalizeRiskRegister(NormalizeRiskRegisterRequest request, StreamObserver<NormalizeRiskRegisterResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeRiskRegister(request));
    }

    /**
     * Explains a risk register.
     */
    @Override
    public void explainRiskRegisterDecision(ExplainRiskRegisterDecisionRequest request, StreamObserver<ExplainRiskRegisterDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainRiskRegisterDecision(request));
    }

    /**
     * Scores a risk register.
     */
    @Override
    public void scoreRiskRegister(ScoreRiskRegisterRequest request, StreamObserver<ScoreRiskRegisterResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreRiskRegister(request));
    }

    /**
     * Lints a risk register.
     */
    @Override
    public void lintRiskRegisters(LintRiskRegistersRequest request, StreamObserver<LintRiskRegistersResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintRiskRegisters(request));
    }

    /**
     * Returns a risk register.
     */
    @Override
    public void getRiskRegisterRules(GetRiskRegisterRulesRequest request, StreamObserver<GetRiskRegisterRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getRiskRegisterRules(request));
    }
}
