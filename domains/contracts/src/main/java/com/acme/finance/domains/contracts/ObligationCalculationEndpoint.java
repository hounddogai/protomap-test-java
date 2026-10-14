package com.acme.finance.domains.contracts;

import com.acme.contracts.v1.CalculateObligationRequest;
import com.acme.contracts.v1.CalculateObligationResponse;
import com.acme.contracts.v1.CompareObligationOptionsRequest;
import com.acme.contracts.v1.CompareObligationOptionsResponse;
import com.acme.contracts.v1.EstimateObligationRequest;
import com.acme.contracts.v1.EstimateObligationResponse;
import com.acme.contracts.v1.ExplainObligationCalculationRequest;
import com.acme.contracts.v1.ExplainObligationCalculationResponse;
import com.acme.contracts.v1.GetObligationRatesRequest;
import com.acme.contracts.v1.GetObligationRatesResponse;
import com.acme.contracts.v1.ObligationCalculationServiceGrpc;
import com.acme.contracts.v1.QuoteObligationRequest;
import com.acme.contracts.v1.QuoteObligationResponse;
import com.acme.contracts.v1.RecalculateObligationsRequest;
import com.acme.contracts.v1.RecalculateObligationsResponse;
import com.acme.contracts.v1.SimulateObligationRequest;
import com.acme.contracts.v1.SimulateObligationResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of an obligation, such as totals, taxes, and estimates.
 */
@GrpcService
public class ObligationCalculationEndpoint extends ObligationCalculationServiceGrpc.ObligationCalculationServiceImplBase {
    protected final ObligationCalculationApplication application;

    public ObligationCalculationEndpoint(ObligationCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates an obligation.
     */
    @Override
    public void calculateObligation(CalculateObligationRequest request, StreamObserver<CalculateObligationResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateObligation(request));
    }

    /**
     * Estimates an obligation.
     */
    @Override
    public void estimateObligation(EstimateObligationRequest request, StreamObserver<EstimateObligationResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateObligation(request));
    }

    /**
     * Quotes an obligation.
     */
    @Override
    public void quoteObligation(QuoteObligationRequest request, StreamObserver<QuoteObligationResponse> observer) {
        RpcSupport.unary(observer, () -> application.quoteObligation(request));
    }

    /**
     * Compares an obligation.
     */
    @Override
    public void compareObligationOptions(CompareObligationOptionsRequest request, StreamObserver<CompareObligationOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareObligationOptions(request));
    }

    /**
     * Recalculates an obligation.
     */
    @Override
    public void recalculateObligations(RecalculateObligationsRequest request, StreamObserver<RecalculateObligationsResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateObligations(request));
    }

    /**
     * Simulates an obligation.
     */
    @Override
    public void simulateObligation(SimulateObligationRequest request, StreamObserver<SimulateObligationResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateObligation(request));
    }

    /**
     * Explains an obligation.
     */
    @Override
    public void explainObligationCalculation(ExplainObligationCalculationRequest request, StreamObserver<ExplainObligationCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainObligationCalculation(request));
    }

    /**
     * Returns an obligation.
     */
    @Override
    public void getObligationRates(GetObligationRatesRequest request, StreamObserver<GetObligationRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getObligationRates(request));
    }
}
