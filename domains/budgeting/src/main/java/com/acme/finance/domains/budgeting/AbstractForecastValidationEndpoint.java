package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.CheckForecastEligibilityRequest;
import com.acme.budgeting.v1.CheckForecastEligibilityResponse;
import com.acme.budgeting.v1.ExplainForecastDecisionRequest;
import com.acme.budgeting.v1.ExplainForecastDecisionResponse;
import com.acme.budgeting.v1.ForecastValidationServiceGrpc;
import com.acme.budgeting.v1.NormalizeForecastRequest;
import com.acme.budgeting.v1.NormalizeForecastResponse;
import com.acme.budgeting.v1.PreviewForecastRequest;
import com.acme.budgeting.v1.PreviewForecastResponse;
import com.acme.budgeting.v1.ScoreForecastRequest;
import com.acme.budgeting.v1.ScoreForecastResponse;
import com.acme.budgeting.v1.ValidateForecastRequest;
import com.acme.budgeting.v1.ValidateForecastResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Checks forecasts against the rules before anything acts on them.
 */
public abstract class AbstractForecastValidationEndpoint extends ForecastValidationServiceGrpc.ForecastValidationServiceImplBase {
    protected final ForecastValidationApplication application;

    protected AbstractForecastValidationEndpoint(ForecastValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a forecast.
     */
    @Override
    public void validateForecast(ValidateForecastRequest request, StreamObserver<ValidateForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateForecast(request));
    }

    /**
     * Previews a forecast.
     */
    @Override
    public void previewForecast(PreviewForecastRequest request, StreamObserver<PreviewForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewForecast(request));
    }

    /**
     * Checks a forecast.
     */
    @Override
    public void checkForecastEligibility(CheckForecastEligibilityRequest request, StreamObserver<CheckForecastEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkForecastEligibility(request));
    }

    /**
     * Normalizes a forecast.
     */
    @Override
    public void normalizeForecast(NormalizeForecastRequest request, StreamObserver<NormalizeForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeForecast(request));
    }

    /**
     * Explains a forecast.
     */
    @Override
    public void explainForecastDecision(ExplainForecastDecisionRequest request, StreamObserver<ExplainForecastDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainForecastDecision(request));
    }

    /**
     * Scores a forecast.
     */
    @Override
    public void scoreForecast(ScoreForecastRequest request, StreamObserver<ScoreForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreForecast(request));
    }
}
