package com.acme.finance.domains.treasury;

import com.acme.finance.platform.RpcSupport;
import com.acme.treasury.v1.CheckLiquidityForecastEligibilityRequest;
import com.acme.treasury.v1.CheckLiquidityForecastEligibilityResponse;
import com.acme.treasury.v1.ExplainLiquidityForecastDecisionRequest;
import com.acme.treasury.v1.ExplainLiquidityForecastDecisionResponse;
import com.acme.treasury.v1.GetLiquidityForecastRulesRequest;
import com.acme.treasury.v1.GetLiquidityForecastRulesResponse;
import com.acme.treasury.v1.LintLiquidityForecastsRequest;
import com.acme.treasury.v1.LintLiquidityForecastsResponse;
import com.acme.treasury.v1.LiquidityForecastValidationServiceGrpc;
import com.acme.treasury.v1.PreviewLiquidityForecastRequest;
import com.acme.treasury.v1.PreviewLiquidityForecastResponse;
import com.acme.treasury.v1.ScoreLiquidityForecastRequest;
import com.acme.treasury.v1.ScoreLiquidityForecastResponse;
import com.acme.treasury.v1.ValidateLiquidityForecastRequest;
import com.acme.treasury.v1.ValidateLiquidityForecastResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks liquidity forecasts against the rules before anything acts on them.
 */
@GrpcService
public class LiquidityForecastValidationEndpoint extends LiquidityForecastValidationServiceGrpc.LiquidityForecastValidationServiceImplBase {
    protected final LiquidityForecastValidationApplication application;

    public LiquidityForecastValidationEndpoint(LiquidityForecastValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a liquidity forecast.
     */
    @Override
    public void validateLiquidityForecast(ValidateLiquidityForecastRequest request, StreamObserver<ValidateLiquidityForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateLiquidityForecast(request));
    }

    /**
     * Previews a liquidity forecast.
     */
    @Override
    public void previewLiquidityForecast(PreviewLiquidityForecastRequest request, StreamObserver<PreviewLiquidityForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewLiquidityForecast(request));
    }

    /**
     * Checks a liquidity forecast.
     */
    @Override
    public void checkLiquidityForecastEligibility(CheckLiquidityForecastEligibilityRequest request, StreamObserver<CheckLiquidityForecastEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkLiquidityForecastEligibility(request));
    }

    /**
     * Explains a liquidity forecast.
     */
    @Override
    public void explainLiquidityForecastDecision(ExplainLiquidityForecastDecisionRequest request, StreamObserver<ExplainLiquidityForecastDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainLiquidityForecastDecision(request));
    }

    /**
     * Scores a liquidity forecast.
     */
    @Override
    public void scoreLiquidityForecast(ScoreLiquidityForecastRequest request, StreamObserver<ScoreLiquidityForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreLiquidityForecast(request));
    }

    /**
     * Lints a liquidity forecast.
     */
    @Override
    public void lintLiquidityForecasts(LintLiquidityForecastsRequest request, StreamObserver<LintLiquidityForecastsResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintLiquidityForecasts(request));
    }

    /**
     * Returns a liquidity forecast.
     */
    @Override
    public void getLiquidityForecastRules(GetLiquidityForecastRulesRequest request, StreamObserver<GetLiquidityForecastRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getLiquidityForecastRules(request));
    }
}
