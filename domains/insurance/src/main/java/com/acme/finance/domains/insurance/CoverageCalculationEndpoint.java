package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.CalculateCoverageRequest;
import com.acme.insurance.v1.CalculateCoverageResponse;
import com.acme.insurance.v1.CompareCoverageOptionsRequest;
import com.acme.insurance.v1.CompareCoverageOptionsResponse;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.EstimateCoverageRequest;
import com.acme.insurance.v1.EstimateCoverageResponse;
import com.acme.insurance.v1.ExplainCoverageCalculationRequest;
import com.acme.insurance.v1.ExplainCoverageCalculationResponse;
import com.acme.insurance.v1.QuoteCoverageRequest;
import com.acme.insurance.v1.QuoteCoverageResponse;
import com.acme.insurance.v1.RecalculateCoveragesRequest;
import com.acme.insurance.v1.RecalculateCoveragesResponse;
import com.acme.insurance.v1.SimulateCoverageRequest;
import com.acme.insurance.v1.SimulateCoverageResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of a coverage, such as totals, taxes, and estimates.
 */
@GrpcService
public class CoverageCalculationEndpoint extends CoverageCalculationServiceGrpc.CoverageCalculationServiceImplBase {
    protected final CoverageCalculationApplication application;

    public CoverageCalculationEndpoint(CoverageCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a coverage.
     */
    @Override
    public void calculateCoverage(CalculateCoverageRequest request, StreamObserver<CalculateCoverageResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateCoverage(request));
    }

    /**
     * Estimates a coverage.
     */
    @Override
    public void estimateCoverage(EstimateCoverageRequest request, StreamObserver<EstimateCoverageResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateCoverage(request));
    }

    /**
     * Quotes a coverage.
     */
    @Override
    public void quoteCoverage(QuoteCoverageRequest request, StreamObserver<QuoteCoverageResponse> observer) {
        RpcSupport.unary(observer, () -> application.quoteCoverage(request));
    }

    /**
     * Compares a coverage.
     */
    @Override
    public void compareCoverageOptions(CompareCoverageOptionsRequest request, StreamObserver<CompareCoverageOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareCoverageOptions(request));
    }

    /**
     * Recalculates a coverage.
     */
    @Override
    public void recalculateCoverages(RecalculateCoveragesRequest request, StreamObserver<RecalculateCoveragesResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateCoverages(request));
    }

    /**
     * Simulates a coverage.
     */
    @Override
    public void simulateCoverage(SimulateCoverageRequest request, StreamObserver<SimulateCoverageResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateCoverage(request));
    }

    /**
     * Explains a coverage.
     */
    @Override
    public void explainCoverageCalculation(ExplainCoverageCalculationRequest request, StreamObserver<ExplainCoverageCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainCoverageCalculation(request));
    }
}
