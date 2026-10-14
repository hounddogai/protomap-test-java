package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.CalculateForecastRequest;
import com.acme.budgeting.v1.CalculateForecastResponse;
import com.acme.budgeting.v1.CompareForecastOptionsRequest;
import com.acme.budgeting.v1.CompareForecastOptionsResponse;
import com.acme.budgeting.v1.EstimateForecastRequest;
import com.acme.budgeting.v1.EstimateForecastResponse;
import com.acme.budgeting.v1.ExplainForecastCalculationRequest;
import com.acme.budgeting.v1.ExplainForecastCalculationResponse;
import com.acme.budgeting.v1.ForecastCalculationServiceGrpc;
import com.acme.budgeting.v1.GetForecastRatesRequest;
import com.acme.budgeting.v1.GetForecastRatesResponse;
import com.acme.budgeting.v1.QuoteForecastRequest;
import com.acme.budgeting.v1.QuoteForecastResponse;
import com.acme.budgeting.v1.RecalculateForecastsRequest;
import com.acme.budgeting.v1.RecalculateForecastsResponse;
import com.acme.budgeting.v1.SimulateForecastRequest;
import com.acme.budgeting.v1.SimulateForecastResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of a forecast, such as totals, taxes, and estimates.
 */
@GrpcService
public class ForecastCalculationEndpoint extends ForecastCalculationServiceGrpc.ForecastCalculationServiceImplBase {
    protected final ForecastCalculationApplication application;

    public ForecastCalculationEndpoint(ForecastCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a forecast.
     */
    @Override
    public void calculateForecast(CalculateForecastRequest request, StreamObserver<CalculateForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateForecast(request));
    }

    /**
     * Estimates a forecast.
     */
    @Override
    public void estimateForecast(EstimateForecastRequest request, StreamObserver<EstimateForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateForecast(request));
    }

    /**
     * Quotes a forecast.
     */
    @Override
    public void quoteForecast(QuoteForecastRequest request, StreamObserver<QuoteForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.quoteForecast(request));
    }

    /**
     * Compares a forecast.
     */
    @Override
    public void compareForecastOptions(CompareForecastOptionsRequest request, StreamObserver<CompareForecastOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareForecastOptions(request));
    }

    /**
     * Recalculates a forecast.
     */
    @Override
    public void recalculateForecasts(RecalculateForecastsRequest request, StreamObserver<RecalculateForecastsResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateForecasts(request));
    }

    /**
     * Simulates a forecast.
     */
    @Override
    public void simulateForecast(SimulateForecastRequest request, StreamObserver<SimulateForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateForecast(request));
    }

    /**
     * Explains a forecast.
     */
    @Override
    public void explainForecastCalculation(ExplainForecastCalculationRequest request, StreamObserver<ExplainForecastCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainForecastCalculation(request));
    }

    /**
     * Returns a forecast.
     */
    @Override
    public void getForecastRates(GetForecastRatesRequest request, StreamObserver<GetForecastRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getForecastRates(request));
    }
}
