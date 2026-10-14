package com.acme.finance.domains.compliance;

import com.acme.compliance.v1.CalculateComplianceCaseRequest;
import com.acme.compliance.v1.CalculateComplianceCaseResponse;
import com.acme.compliance.v1.CompareComplianceCaseOptionsRequest;
import com.acme.compliance.v1.CompareComplianceCaseOptionsResponse;
import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
import com.acme.compliance.v1.EstimateComplianceCaseRequest;
import com.acme.compliance.v1.EstimateComplianceCaseResponse;
import com.acme.compliance.v1.ExplainComplianceCaseCalculationRequest;
import com.acme.compliance.v1.ExplainComplianceCaseCalculationResponse;
import com.acme.compliance.v1.GetComplianceCaseRatesRequest;
import com.acme.compliance.v1.GetComplianceCaseRatesResponse;
import com.acme.compliance.v1.QuoteComplianceCaseRequest;
import com.acme.compliance.v1.QuoteComplianceCaseResponse;
import com.acme.compliance.v1.RecalculateComplianceCasesRequest;
import com.acme.compliance.v1.RecalculateComplianceCasesResponse;
import com.acme.compliance.v1.SimulateComplianceCaseRequest;
import com.acme.compliance.v1.SimulateComplianceCaseResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Computes the figures of a compliance case, such as totals, taxes, and estimates.
 */
public abstract class AbstractComplianceCaseCalculationEndpoint extends ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceImplBase {
    protected final ComplianceCaseCalculationApplication application;

    protected AbstractComplianceCaseCalculationEndpoint(ComplianceCaseCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a compliance case.
     */
    @Override
    public void calculateComplianceCase(CalculateComplianceCaseRequest request, StreamObserver<CalculateComplianceCaseResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateComplianceCase(request));
    }

    /**
     * Estimates a compliance case.
     */
    @Override
    public void estimateComplianceCase(EstimateComplianceCaseRequest request, StreamObserver<EstimateComplianceCaseResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateComplianceCase(request));
    }

    /**
     * Quotes a compliance case.
     */
    @Override
    public void quoteComplianceCase(QuoteComplianceCaseRequest request, StreamObserver<QuoteComplianceCaseResponse> observer) {
        RpcSupport.unary(observer, () -> application.quoteComplianceCase(request));
    }

    /**
     * Compares a compliance case.
     */
    @Override
    public void compareComplianceCaseOptions(CompareComplianceCaseOptionsRequest request, StreamObserver<CompareComplianceCaseOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareComplianceCaseOptions(request));
    }

    /**
     * Recalculates a compliance case.
     */
    @Override
    public void recalculateComplianceCases(RecalculateComplianceCasesRequest request, StreamObserver<RecalculateComplianceCasesResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateComplianceCases(request));
    }

    /**
     * Simulates a compliance case.
     */
    @Override
    public void simulateComplianceCase(SimulateComplianceCaseRequest request, StreamObserver<SimulateComplianceCaseResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateComplianceCase(request));
    }

    /**
     * Explains a compliance case.
     */
    @Override
    public void explainComplianceCaseCalculation(ExplainComplianceCaseCalculationRequest request, StreamObserver<ExplainComplianceCaseCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainComplianceCaseCalculation(request));
    }

    /**
     * Returns a compliance case.
     */
    @Override
    public void getComplianceCaseRates(GetComplianceCaseRatesRequest request, StreamObserver<GetComplianceCaseRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getComplianceCaseRates(request));
    }
}
