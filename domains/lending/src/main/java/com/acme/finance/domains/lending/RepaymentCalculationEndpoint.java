package com.acme.finance.domains.lending;

import com.acme.finance.platform.RpcSupport;
import com.acme.lending.v1.CalculateRepaymentRequest;
import com.acme.lending.v1.CalculateRepaymentResponse;
import com.acme.lending.v1.CompareRepaymentOptionsRequest;
import com.acme.lending.v1.CompareRepaymentOptionsResponse;
import com.acme.lending.v1.EstimateRepaymentRequest;
import com.acme.lending.v1.EstimateRepaymentResponse;
import com.acme.lending.v1.ExplainRepaymentCalculationRequest;
import com.acme.lending.v1.ExplainRepaymentCalculationResponse;
import com.acme.lending.v1.GetRepaymentRatesRequest;
import com.acme.lending.v1.GetRepaymentRatesResponse;
import com.acme.lending.v1.QuoteRepaymentRequest;
import com.acme.lending.v1.QuoteRepaymentResponse;
import com.acme.lending.v1.RecalculateRepaymentsRequest;
import com.acme.lending.v1.RecalculateRepaymentsResponse;
import com.acme.lending.v1.RepaymentCalculationServiceGrpc;
import com.acme.lending.v1.SimulateRepaymentRequest;
import com.acme.lending.v1.SimulateRepaymentResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of a repayment, such as totals, taxes, and estimates.
 */
@GrpcService
public class RepaymentCalculationEndpoint extends RepaymentCalculationServiceGrpc.RepaymentCalculationServiceImplBase {
    protected final RepaymentCalculationApplication application;

    public RepaymentCalculationEndpoint(RepaymentCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a repayment.
     */
    @Override
    public void calculateRepayment(CalculateRepaymentRequest request, StreamObserver<CalculateRepaymentResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateRepayment(request));
    }

    /**
     * Estimates a repayment.
     */
    @Override
    public void estimateRepayment(EstimateRepaymentRequest request, StreamObserver<EstimateRepaymentResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateRepayment(request));
    }

    /**
     * Quotes a repayment.
     */
    @Override
    public void quoteRepayment(QuoteRepaymentRequest request, StreamObserver<QuoteRepaymentResponse> observer) {
        RpcSupport.unary(observer, () -> application.quoteRepayment(request));
    }

    /**
     * Compares a repayment.
     */
    @Override
    public void compareRepaymentOptions(CompareRepaymentOptionsRequest request, StreamObserver<CompareRepaymentOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareRepaymentOptions(request));
    }

    /**
     * Recalculates a repayment.
     */
    @Override
    public void recalculateRepayments(RecalculateRepaymentsRequest request, StreamObserver<RecalculateRepaymentsResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateRepayments(request));
    }

    /**
     * Simulates a repayment.
     */
    @Override
    public void simulateRepayment(SimulateRepaymentRequest request, StreamObserver<SimulateRepaymentResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateRepayment(request));
    }

    /**
     * Explains a repayment.
     */
    @Override
    public void explainRepaymentCalculation(ExplainRepaymentCalculationRequest request, StreamObserver<ExplainRepaymentCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainRepaymentCalculation(request));
    }

    /**
     * Returns a repayment.
     */
    @Override
    public void getRepaymentRates(GetRepaymentRatesRequest request, StreamObserver<GetRepaymentRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getRepaymentRates(request));
    }
}
