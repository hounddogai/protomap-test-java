package com.acme.finance.domains.treasury;

import com.acme.finance.platform.RpcSupport;
import com.acme.treasury.v1.CashPositionValidationServiceGrpc;
import com.acme.treasury.v1.CheckCashPositionEligibilityRequest;
import com.acme.treasury.v1.CheckCashPositionEligibilityResponse;
import com.acme.treasury.v1.ExplainCashPositionDecisionRequest;
import com.acme.treasury.v1.ExplainCashPositionDecisionResponse;
import com.acme.treasury.v1.GetCashPositionRulesRequest;
import com.acme.treasury.v1.GetCashPositionRulesResponse;
import com.acme.treasury.v1.LintCashPositionsRequest;
import com.acme.treasury.v1.LintCashPositionsResponse;
import com.acme.treasury.v1.PreviewCashPositionRequest;
import com.acme.treasury.v1.PreviewCashPositionResponse;
import com.acme.treasury.v1.ScoreCashPositionRequest;
import com.acme.treasury.v1.ScoreCashPositionResponse;
import com.acme.treasury.v1.ValidateCashPositionRequest;
import com.acme.treasury.v1.ValidateCashPositionResponse;
import io.grpc.stub.StreamObserver;

/**
 * Checks cash positions against the rules before anything acts on them.
 */
public abstract class AbstractCashPositionValidationEndpoint extends CashPositionValidationServiceGrpc.CashPositionValidationServiceImplBase {
    protected final CashPositionValidationApplication application;

    protected AbstractCashPositionValidationEndpoint(CashPositionValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a cash position.
     */
    @Override
    public void validateCashPosition(ValidateCashPositionRequest request, StreamObserver<ValidateCashPositionResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateCashPosition(request));
    }

    /**
     * Previews a cash position.
     */
    @Override
    public void previewCashPosition(PreviewCashPositionRequest request, StreamObserver<PreviewCashPositionResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewCashPosition(request));
    }

    /**
     * Checks a cash position.
     */
    @Override
    public void checkCashPositionEligibility(CheckCashPositionEligibilityRequest request, StreamObserver<CheckCashPositionEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkCashPositionEligibility(request));
    }

    /**
     * Explains a cash position.
     */
    @Override
    public void explainCashPositionDecision(ExplainCashPositionDecisionRequest request, StreamObserver<ExplainCashPositionDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainCashPositionDecision(request));
    }

    /**
     * Scores a cash position.
     */
    @Override
    public void scoreCashPosition(ScoreCashPositionRequest request, StreamObserver<ScoreCashPositionResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreCashPosition(request));
    }

    /**
     * Lints a cash position.
     */
    @Override
    public void lintCashPositions(LintCashPositionsRequest request, StreamObserver<LintCashPositionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintCashPositions(request));
    }

    /**
     * Returns a cash position.
     */
    @Override
    public void getCashPositionRules(GetCashPositionRulesRequest request, StreamObserver<GetCashPositionRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getCashPositionRules(request));
    }
}
