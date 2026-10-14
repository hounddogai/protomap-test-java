package com.acme.finance.domains.risk;

import com.acme.finance.platform.RpcSupport;
import com.acme.risk.v1.CalculateExposureRequest;
import com.acme.risk.v1.CalculateExposureResponse;
import com.acme.risk.v1.CompareExposureOptionsRequest;
import com.acme.risk.v1.CompareExposureOptionsResponse;
import com.acme.risk.v1.EstimateExposureRequest;
import com.acme.risk.v1.EstimateExposureResponse;
import com.acme.risk.v1.ExplainExposureCalculationRequest;
import com.acme.risk.v1.ExplainExposureCalculationResponse;
import com.acme.risk.v1.ExposureCalculationServiceGrpc;
import com.acme.risk.v1.GetExposureRatesRequest;
import com.acme.risk.v1.GetExposureRatesResponse;
import com.acme.risk.v1.QuoteExposureRequest;
import com.acme.risk.v1.QuoteExposureResponse;
import com.acme.risk.v1.RecalculateExposuresRequest;
import com.acme.risk.v1.RecalculateExposuresResponse;
import com.acme.risk.v1.SimulateExposureRequest;
import com.acme.risk.v1.SimulateExposureResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of an exposure, such as totals, taxes, and estimates.
 */
@GrpcService
public class ExposureCalculationEndpoint extends ExposureCalculationServiceGrpc.ExposureCalculationServiceImplBase {
    protected final ExposureCalculationApplication application;

    public ExposureCalculationEndpoint(ExposureCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates an exposure.
     */
    @Override
    public void calculateExposure(CalculateExposureRequest request, StreamObserver<CalculateExposureResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateExposure(request));
    }

    /**
     * Estimates an exposure.
     */
    @Override
    public void estimateExposure(EstimateExposureRequest request, StreamObserver<EstimateExposureResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateExposure(request));
    }

    /**
     * Quotes an exposure.
     */
    @Override
    public void quoteExposure(QuoteExposureRequest request, StreamObserver<QuoteExposureResponse> observer) {
        RpcSupport.unary(observer, () -> application.quoteExposure(request));
    }

    /**
     * Compares an exposure.
     */
    @Override
    public void compareExposureOptions(CompareExposureOptionsRequest request, StreamObserver<CompareExposureOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareExposureOptions(request));
    }

    /**
     * Recalculates an exposure.
     */
    @Override
    public void recalculateExposures(RecalculateExposuresRequest request, StreamObserver<RecalculateExposuresResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateExposures(request));
    }

    /**
     * Simulates an exposure.
     */
    @Override
    public void simulateExposure(SimulateExposureRequest request, StreamObserver<SimulateExposureResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateExposure(request));
    }

    /**
     * Explains an exposure.
     */
    @Override
    public void explainExposureCalculation(ExplainExposureCalculationRequest request, StreamObserver<ExplainExposureCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainExposureCalculation(request));
    }

    /**
     * Returns an exposure.
     */
    @Override
    public void getExposureRates(GetExposureRatesRequest request, StreamObserver<GetExposureRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getExposureRates(request));
    }
}
