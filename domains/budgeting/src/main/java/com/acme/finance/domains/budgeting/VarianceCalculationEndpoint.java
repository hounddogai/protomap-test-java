package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.CalculateVarianceRequest;
import com.acme.budgeting.v1.CalculateVarianceResponse;
import com.acme.budgeting.v1.CompareVarianceOptionsRequest;
import com.acme.budgeting.v1.CompareVarianceOptionsResponse;
import com.acme.budgeting.v1.EstimateVarianceRequest;
import com.acme.budgeting.v1.EstimateVarianceResponse;
import com.acme.budgeting.v1.ExplainVarianceCalculationRequest;
import com.acme.budgeting.v1.ExplainVarianceCalculationResponse;
import com.acme.budgeting.v1.GetVarianceRatesRequest;
import com.acme.budgeting.v1.GetVarianceRatesResponse;
import com.acme.budgeting.v1.QuoteVarianceRequest;
import com.acme.budgeting.v1.QuoteVarianceResponse;
import com.acme.budgeting.v1.RecalculateVariancesRequest;
import com.acme.budgeting.v1.RecalculateVariancesResponse;
import com.acme.budgeting.v1.SimulateVarianceRequest;
import com.acme.budgeting.v1.SimulateVarianceResponse;
import com.acme.budgeting.v1.VarianceCalculationServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of a variance, such as totals, taxes, and estimates.
 */
@GrpcService
public class VarianceCalculationEndpoint extends VarianceCalculationServiceGrpc.VarianceCalculationServiceImplBase {
    protected final VarianceCalculationApplication application;

    public VarianceCalculationEndpoint(VarianceCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a variance.
     */
    @Override
    public void calculateVariance(CalculateVarianceRequest request, StreamObserver<CalculateVarianceResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateVariance(request));
    }

    /**
     * Estimates a variance.
     */
    @Override
    public void estimateVariance(EstimateVarianceRequest request, StreamObserver<EstimateVarianceResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateVariance(request));
    }

    /**
     * Quotes a variance.
     */
    @Override
    public void quoteVariance(QuoteVarianceRequest request, StreamObserver<QuoteVarianceResponse> observer) {
        RpcSupport.unary(observer, () -> application.quoteVariance(request));
    }

    /**
     * Compares a variance.
     */
    @Override
    public void compareVarianceOptions(CompareVarianceOptionsRequest request, StreamObserver<CompareVarianceOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareVarianceOptions(request));
    }

    /**
     * Recalculates a variance.
     */
    @Override
    public void recalculateVariances(RecalculateVariancesRequest request, StreamObserver<RecalculateVariancesResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateVariances(request));
    }

    /**
     * Simulates a variance.
     */
    @Override
    public void simulateVariance(SimulateVarianceRequest request, StreamObserver<SimulateVarianceResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateVariance(request));
    }

    /**
     * Explains a variance.
     */
    @Override
    public void explainVarianceCalculation(ExplainVarianceCalculationRequest request, StreamObserver<ExplainVarianceCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainVarianceCalculation(request));
    }

    /**
     * Returns a variance.
     */
    @Override
    public void getVarianceRates(GetVarianceRatesRequest request, StreamObserver<GetVarianceRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getVarianceRates(request));
    }
}
