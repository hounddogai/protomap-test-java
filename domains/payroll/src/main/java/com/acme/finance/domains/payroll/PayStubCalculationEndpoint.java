package com.acme.finance.domains.payroll;

import com.acme.finance.platform.RpcSupport;
import com.acme.payroll.v1.CalculatePayStubRequest;
import com.acme.payroll.v1.CalculatePayStubResponse;
import com.acme.payroll.v1.ComparePayStubOptionsRequest;
import com.acme.payroll.v1.ComparePayStubOptionsResponse;
import com.acme.payroll.v1.EstimatePayStubRequest;
import com.acme.payroll.v1.EstimatePayStubResponse;
import com.acme.payroll.v1.ExplainPayStubCalculationRequest;
import com.acme.payroll.v1.ExplainPayStubCalculationResponse;
import com.acme.payroll.v1.GetPayStubRatesRequest;
import com.acme.payroll.v1.GetPayStubRatesResponse;
import com.acme.payroll.v1.PayStubCalculationServiceGrpc;
import com.acme.payroll.v1.QuotePayStubRequest;
import com.acme.payroll.v1.QuotePayStubResponse;
import com.acme.payroll.v1.RecalculatePayStubsRequest;
import com.acme.payroll.v1.RecalculatePayStubsResponse;
import com.acme.payroll.v1.SimulatePayStubRequest;
import com.acme.payroll.v1.SimulatePayStubResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of a pay stub, such as totals, taxes, and estimates.
 */
@GrpcService
public class PayStubCalculationEndpoint extends PayStubCalculationServiceGrpc.PayStubCalculationServiceImplBase {
    protected final PayStubCalculationApplication application;

    public PayStubCalculationEndpoint(PayStubCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a pay stub.
     */
    @Override
    public void calculatePayStub(CalculatePayStubRequest request, StreamObserver<CalculatePayStubResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculatePayStub(request));
    }

    /**
     * Estimates a pay stub.
     */
    @Override
    public void estimatePayStub(EstimatePayStubRequest request, StreamObserver<EstimatePayStubResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimatePayStub(request));
    }

    /**
     * Quotes a pay stub.
     */
    @Override
    public void quotePayStub(QuotePayStubRequest request, StreamObserver<QuotePayStubResponse> observer) {
        RpcSupport.unary(observer, () -> application.quotePayStub(request));
    }

    /**
     * Compares a pay stub.
     */
    @Override
    public void comparePayStubOptions(ComparePayStubOptionsRequest request, StreamObserver<ComparePayStubOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.comparePayStubOptions(request));
    }

    /**
     * Recalculates a pay stub.
     */
    @Override
    public void recalculatePayStubs(RecalculatePayStubsRequest request, StreamObserver<RecalculatePayStubsResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculatePayStubs(request));
    }

    /**
     * Simulates a pay stub.
     */
    @Override
    public void simulatePayStub(SimulatePayStubRequest request, StreamObserver<SimulatePayStubResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulatePayStub(request));
    }

    /**
     * Explains a pay stub.
     */
    @Override
    public void explainPayStubCalculation(ExplainPayStubCalculationRequest request, StreamObserver<ExplainPayStubCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainPayStubCalculation(request));
    }

    /**
     * Returns a pay stub.
     */
    @Override
    public void getPayStubRates(GetPayStubRatesRequest request, StreamObserver<GetPayStubRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getPayStubRates(request));
    }
}
