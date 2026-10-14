package com.acme.finance.domains.assets;

import com.acme.assets.v1.CalculateDepreciationRequest;
import com.acme.assets.v1.CalculateDepreciationResponse;
import com.acme.assets.v1.CompareDepreciationOptionsRequest;
import com.acme.assets.v1.CompareDepreciationOptionsResponse;
import com.acme.assets.v1.DepreciationCalculationServiceGrpc;
import com.acme.assets.v1.EstimateDepreciationRequest;
import com.acme.assets.v1.EstimateDepreciationResponse;
import com.acme.assets.v1.ExplainDepreciationCalculationRequest;
import com.acme.assets.v1.ExplainDepreciationCalculationResponse;
import com.acme.assets.v1.GetDepreciationRatesRequest;
import com.acme.assets.v1.GetDepreciationRatesResponse;
import com.acme.assets.v1.QuoteDepreciationRequest;
import com.acme.assets.v1.QuoteDepreciationResponse;
import com.acme.assets.v1.RecalculateDepreciationsRequest;
import com.acme.assets.v1.RecalculateDepreciationsResponse;
import com.acme.assets.v1.SimulateDepreciationRequest;
import com.acme.assets.v1.SimulateDepreciationResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Computes the figures of a depreciation, such as totals, taxes, and estimates.
 */
public abstract class AbstractDepreciationCalculationEndpoint extends DepreciationCalculationServiceGrpc.DepreciationCalculationServiceImplBase {
    protected final DepreciationCalculationApplication application;

    protected AbstractDepreciationCalculationEndpoint(DepreciationCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a depreciation.
     */
    @Override
    public void calculateDepreciation(CalculateDepreciationRequest request, StreamObserver<CalculateDepreciationResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateDepreciation(request));
    }

    /**
     * Estimates a depreciation.
     */
    @Override
    public void estimateDepreciation(EstimateDepreciationRequest request, StreamObserver<EstimateDepreciationResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateDepreciation(request));
    }

    /**
     * Quotes a depreciation.
     */
    @Override
    public void quoteDepreciation(QuoteDepreciationRequest request, StreamObserver<QuoteDepreciationResponse> observer) {
        RpcSupport.unary(observer, () -> application.quoteDepreciation(request));
    }

    /**
     * Compares a depreciation.
     */
    @Override
    public void compareDepreciationOptions(CompareDepreciationOptionsRequest request, StreamObserver<CompareDepreciationOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareDepreciationOptions(request));
    }

    /**
     * Recalculates a depreciation.
     */
    @Override
    public void recalculateDepreciations(RecalculateDepreciationsRequest request, StreamObserver<RecalculateDepreciationsResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateDepreciations(request));
    }

    /**
     * Simulates a depreciation.
     */
    @Override
    public void simulateDepreciation(SimulateDepreciationRequest request, StreamObserver<SimulateDepreciationResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateDepreciation(request));
    }

    /**
     * Explains a depreciation.
     */
    @Override
    public void explainDepreciationCalculation(ExplainDepreciationCalculationRequest request, StreamObserver<ExplainDepreciationCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainDepreciationCalculation(request));
    }

    /**
     * Returns a depreciation.
     */
    @Override
    public void getDepreciationRates(GetDepreciationRatesRequest request, StreamObserver<GetDepreciationRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getDepreciationRates(request));
    }
}
