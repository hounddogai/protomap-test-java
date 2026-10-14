package com.acme.finance.domains.lending;

import com.acme.finance.platform.RpcSupport;
import com.acme.lending.v1.CalculateCollateralRequest;
import com.acme.lending.v1.CalculateCollateralResponse;
import com.acme.lending.v1.CollateralCalculationServiceGrpc;
import com.acme.lending.v1.CompareCollateralOptionsRequest;
import com.acme.lending.v1.CompareCollateralOptionsResponse;
import com.acme.lending.v1.EstimateCollateralRequest;
import com.acme.lending.v1.EstimateCollateralResponse;
import com.acme.lending.v1.GetCollateralRatesRequest;
import com.acme.lending.v1.GetCollateralRatesResponse;
import com.acme.lending.v1.QuoteCollateralRequest;
import com.acme.lending.v1.QuoteCollateralResponse;
import com.acme.lending.v1.SimulateCollateralRequest;
import com.acme.lending.v1.SimulateCollateralResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of a collateral, such as totals, taxes, and estimates.
 */
@GrpcService
public class CollateralCalculationEndpoint extends CollateralCalculationServiceGrpc.CollateralCalculationServiceImplBase {
    protected final CollateralCalculationApplication application;

    public CollateralCalculationEndpoint(CollateralCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a collateral.
     */
    @Override
    public void calculateCollateral(CalculateCollateralRequest request, StreamObserver<CalculateCollateralResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateCollateral(request));
    }

    /**
     * Estimates a collateral.
     */
    @Override
    public void estimateCollateral(EstimateCollateralRequest request, StreamObserver<EstimateCollateralResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateCollateral(request));
    }

    /**
     * Quotes a collateral.
     */
    @Override
    public void quoteCollateral(QuoteCollateralRequest request, StreamObserver<QuoteCollateralResponse> observer) {
        RpcSupport.unary(observer, () -> application.quoteCollateral(request));
    }

    /**
     * Compares a collateral.
     */
    @Override
    public void compareCollateralOptions(CompareCollateralOptionsRequest request, StreamObserver<CompareCollateralOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareCollateralOptions(request));
    }

    /**
     * Simulates a collateral.
     */
    @Override
    public void simulateCollateral(SimulateCollateralRequest request, StreamObserver<SimulateCollateralResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateCollateral(request));
    }

    /**
     * Returns a collateral.
     */
    @Override
    public void getCollateralRates(GetCollateralRatesRequest request, StreamObserver<GetCollateralRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getCollateralRates(request));
    }
}
