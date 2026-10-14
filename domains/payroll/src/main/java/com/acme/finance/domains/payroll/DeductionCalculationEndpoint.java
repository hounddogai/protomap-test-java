package com.acme.finance.domains.payroll;

import com.acme.finance.platform.RpcSupport;
import com.acme.payroll.v1.CalculateDeductionRequest;
import com.acme.payroll.v1.CalculateDeductionResponse;
import com.acme.payroll.v1.CompareDeductionOptionsRequest;
import com.acme.payroll.v1.CompareDeductionOptionsResponse;
import com.acme.payroll.v1.DeductionCalculationServiceGrpc;
import com.acme.payroll.v1.EstimateDeductionRequest;
import com.acme.payroll.v1.EstimateDeductionResponse;
import com.acme.payroll.v1.ExplainDeductionCalculationRequest;
import com.acme.payroll.v1.ExplainDeductionCalculationResponse;
import com.acme.payroll.v1.GetDeductionRatesRequest;
import com.acme.payroll.v1.GetDeductionRatesResponse;
import com.acme.payroll.v1.QuoteDeductionRequest;
import com.acme.payroll.v1.QuoteDeductionResponse;
import com.acme.payroll.v1.RecalculateDeductionsRequest;
import com.acme.payroll.v1.RecalculateDeductionsResponse;
import com.acme.payroll.v1.SimulateDeductionRequest;
import com.acme.payroll.v1.SimulateDeductionResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of a deduction, such as totals, taxes, and estimates.
 */
@GrpcService
public class DeductionCalculationEndpoint extends DeductionCalculationServiceGrpc.DeductionCalculationServiceImplBase {
    protected final DeductionCalculationApplication application;

    public DeductionCalculationEndpoint(DeductionCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a deduction.
     */
    @Override
    public void calculateDeduction(CalculateDeductionRequest request, StreamObserver<CalculateDeductionResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateDeduction(request));
    }

    /**
     * Estimates a deduction.
     */
    @Override
    public void estimateDeduction(EstimateDeductionRequest request, StreamObserver<EstimateDeductionResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateDeduction(request));
    }

    /**
     * Quotes a deduction.
     */
    @Override
    public void quoteDeduction(QuoteDeductionRequest request, StreamObserver<QuoteDeductionResponse> observer) {
        RpcSupport.unary(observer, () -> application.quoteDeduction(request));
    }

    /**
     * Compares a deduction.
     */
    @Override
    public void compareDeductionOptions(CompareDeductionOptionsRequest request, StreamObserver<CompareDeductionOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareDeductionOptions(request));
    }

    /**
     * Recalculates a deduction.
     */
    @Override
    public void recalculateDeductions(RecalculateDeductionsRequest request, StreamObserver<RecalculateDeductionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateDeductions(request));
    }

    /**
     * Simulates a deduction.
     */
    @Override
    public void simulateDeduction(SimulateDeductionRequest request, StreamObserver<SimulateDeductionResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateDeduction(request));
    }

    /**
     * Explains a deduction.
     */
    @Override
    public void explainDeductionCalculation(ExplainDeductionCalculationRequest request, StreamObserver<ExplainDeductionCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainDeductionCalculation(request));
    }

    /**
     * Returns a deduction.
     */
    @Override
    public void getDeductionRates(GetDeductionRatesRequest request, StreamObserver<GetDeductionRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getDeductionRates(request));
    }
}
