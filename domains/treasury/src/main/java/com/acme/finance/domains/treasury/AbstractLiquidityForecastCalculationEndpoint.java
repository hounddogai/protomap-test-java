package com.acme.finance.domains.treasury;

import com.acme.finance.platform.RpcSupport;
import com.acme.treasury.v1.CalculateLiquidityForecastRequest;
import com.acme.treasury.v1.CalculateLiquidityForecastResponse;
import com.acme.treasury.v1.CompareLiquidityForecastOptionsRequest;
import com.acme.treasury.v1.CompareLiquidityForecastOptionsResponse;
import com.acme.treasury.v1.EstimateLiquidityForecastRequest;
import com.acme.treasury.v1.EstimateLiquidityForecastResponse;
import com.acme.treasury.v1.ExplainLiquidityForecastCalculationRequest;
import com.acme.treasury.v1.ExplainLiquidityForecastCalculationResponse;
import com.acme.treasury.v1.GetLiquidityForecastRatesRequest;
import com.acme.treasury.v1.GetLiquidityForecastRatesResponse;
import com.acme.treasury.v1.LiquidityForecastCalculationServiceGrpc;
import com.acme.treasury.v1.QuoteLiquidityForecastRequest;
import com.acme.treasury.v1.QuoteLiquidityForecastResponse;
import com.acme.treasury.v1.RecalculateLiquidityForecastsRequest;
import com.acme.treasury.v1.RecalculateLiquidityForecastsResponse;
import com.acme.treasury.v1.SimulateLiquidityForecastRequest;
import com.acme.treasury.v1.SimulateLiquidityForecastResponse;
import io.grpc.stub.StreamObserver;

/**
 * Computes the figures of a liquidity forecast, such as totals, taxes, and estimates.
 */
public abstract class AbstractLiquidityForecastCalculationEndpoint extends LiquidityForecastCalculationServiceGrpc.LiquidityForecastCalculationServiceImplBase {
    protected final LiquidityForecastCalculationApplication application;

    protected AbstractLiquidityForecastCalculationEndpoint(LiquidityForecastCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a liquidity forecast.
     */
    @Override
    public void calculateLiquidityForecast(CalculateLiquidityForecastRequest request, StreamObserver<CalculateLiquidityForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateLiquidityForecast(request));
    }

    /**
     * Estimates a liquidity forecast.
     */
    @Override
    public void estimateLiquidityForecast(EstimateLiquidityForecastRequest request, StreamObserver<EstimateLiquidityForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateLiquidityForecast(request));
    }

    /**
     * Quotes a liquidity forecast.
     */
    @Override
    public void quoteLiquidityForecast(QuoteLiquidityForecastRequest request, StreamObserver<QuoteLiquidityForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.quoteLiquidityForecast(request));
    }

    /**
     * Compares a liquidity forecast.
     */
    @Override
    public void compareLiquidityForecastOptions(CompareLiquidityForecastOptionsRequest request, StreamObserver<CompareLiquidityForecastOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareLiquidityForecastOptions(request));
    }

    /**
     * Recalculates a liquidity forecast.
     */
    @Override
    public void recalculateLiquidityForecasts(RecalculateLiquidityForecastsRequest request, StreamObserver<RecalculateLiquidityForecastsResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateLiquidityForecasts(request));
    }

    /**
     * Simulates a liquidity forecast.
     */
    @Override
    public void simulateLiquidityForecast(SimulateLiquidityForecastRequest request, StreamObserver<SimulateLiquidityForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateLiquidityForecast(request));
    }

    /**
     * Explains a liquidity forecast.
     */
    @Override
    public void explainLiquidityForecastCalculation(ExplainLiquidityForecastCalculationRequest request, StreamObserver<ExplainLiquidityForecastCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainLiquidityForecastCalculation(request));
    }

    /**
     * Returns a liquidity forecast.
     */
    @Override
    public void getLiquidityForecastRates(GetLiquidityForecastRatesRequest request, StreamObserver<GetLiquidityForecastRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getLiquidityForecastRates(request));
    }
}
