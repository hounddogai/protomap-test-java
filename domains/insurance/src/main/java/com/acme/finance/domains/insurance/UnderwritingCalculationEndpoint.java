package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.CalculateUnderwritingRequest;
import com.acme.insurance.v1.CalculateUnderwritingResponse;
import com.acme.insurance.v1.CompareUnderwritingOptionsRequest;
import com.acme.insurance.v1.CompareUnderwritingOptionsResponse;
import com.acme.insurance.v1.EstimateUnderwritingRequest;
import com.acme.insurance.v1.EstimateUnderwritingResponse;
import com.acme.insurance.v1.ExplainUnderwritingCalculationRequest;
import com.acme.insurance.v1.ExplainUnderwritingCalculationResponse;
import com.acme.insurance.v1.GetUnderwritingRatesRequest;
import com.acme.insurance.v1.GetUnderwritingRatesResponse;
import com.acme.insurance.v1.QuoteUnderwritingRequest;
import com.acme.insurance.v1.QuoteUnderwritingResponse;
import com.acme.insurance.v1.RecalculateUnderwritingsRequest;
import com.acme.insurance.v1.RecalculateUnderwritingsResponse;
import com.acme.insurance.v1.SimulateUnderwritingRequest;
import com.acme.insurance.v1.SimulateUnderwritingResponse;
import com.acme.insurance.v1.UnderwritingCalculationServiceGrpc;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of an underwriting, such as totals, taxes, and estimates.
 */
@GrpcService
public class UnderwritingCalculationEndpoint extends UnderwritingCalculationServiceGrpc.UnderwritingCalculationServiceImplBase {
    protected final UnderwritingCalculationApplication application;

    public UnderwritingCalculationEndpoint(UnderwritingCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates an underwriting.
     */
    @Override
    public void calculateUnderwriting(CalculateUnderwritingRequest request, StreamObserver<CalculateUnderwritingResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateUnderwriting(request));
    }

    /**
     * Estimates an underwriting.
     */
    @Override
    public void estimateUnderwriting(EstimateUnderwritingRequest request, StreamObserver<EstimateUnderwritingResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateUnderwriting(request));
    }

    /**
     * Quotes an underwriting.
     */
    @Override
    public void quoteUnderwriting(QuoteUnderwritingRequest request, StreamObserver<QuoteUnderwritingResponse> observer) {
        RpcSupport.unary(observer, () -> application.quoteUnderwriting(request));
    }

    /**
     * Compares an underwriting.
     */
    @Override
    public void compareUnderwritingOptions(CompareUnderwritingOptionsRequest request, StreamObserver<CompareUnderwritingOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareUnderwritingOptions(request));
    }

    /**
     * Recalculates an underwriting.
     */
    @Override
    public void recalculateUnderwritings(RecalculateUnderwritingsRequest request, StreamObserver<RecalculateUnderwritingsResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateUnderwritings(request));
    }

    /**
     * Simulates an underwriting.
     */
    @Override
    public void simulateUnderwriting(SimulateUnderwritingRequest request, StreamObserver<SimulateUnderwritingResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateUnderwriting(request));
    }

    /**
     * Explains an underwriting.
     */
    @Override
    public void explainUnderwritingCalculation(ExplainUnderwritingCalculationRequest request, StreamObserver<ExplainUnderwritingCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainUnderwritingCalculation(request));
    }

    /**
     * Returns an underwriting.
     */
    @Override
    public void getUnderwritingRates(GetUnderwritingRatesRequest request, StreamObserver<GetUnderwritingRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getUnderwritingRates(request));
    }
}
