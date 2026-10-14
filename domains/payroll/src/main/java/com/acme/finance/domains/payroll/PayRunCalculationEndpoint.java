package com.acme.finance.domains.payroll;

import com.acme.finance.platform.RpcSupport;
import com.acme.payroll.v1.CalculatePayRunRequest;
import com.acme.payroll.v1.CalculatePayRunResponse;
import com.acme.payroll.v1.ComparePayRunOptionsRequest;
import com.acme.payroll.v1.ComparePayRunOptionsResponse;
import com.acme.payroll.v1.EstimatePayRunRequest;
import com.acme.payroll.v1.EstimatePayRunResponse;
import com.acme.payroll.v1.ExplainPayRunCalculationRequest;
import com.acme.payroll.v1.ExplainPayRunCalculationResponse;
import com.acme.payroll.v1.GetPayRunRatesRequest;
import com.acme.payroll.v1.GetPayRunRatesResponse;
import com.acme.payroll.v1.PayRunCalculationServiceGrpc;
import com.acme.payroll.v1.QuotePayRunRequest;
import com.acme.payroll.v1.QuotePayRunResponse;
import com.acme.payroll.v1.SimulatePayRunRequest;
import com.acme.payroll.v1.SimulatePayRunResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of a pay run, such as totals, taxes, and estimates.
 */
@GrpcService
public class PayRunCalculationEndpoint extends PayRunCalculationServiceGrpc.PayRunCalculationServiceImplBase {
    protected final PayRunCalculationApplication application;

    public PayRunCalculationEndpoint(PayRunCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a pay run.
     */
    @Override
    public void calculatePayRun(CalculatePayRunRequest request, StreamObserver<CalculatePayRunResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculatePayRun(request));
    }

    /**
     * Estimates a pay run.
     */
    @Override
    public void estimatePayRun(EstimatePayRunRequest request, StreamObserver<EstimatePayRunResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimatePayRun(request));
    }

    /**
     * Quotes a pay run.
     */
    @Override
    public void quotePayRun(QuotePayRunRequest request, StreamObserver<QuotePayRunResponse> observer) {
        RpcSupport.unary(observer, () -> application.quotePayRun(request));
    }

    /**
     * Compares a pay run.
     */
    @Override
    public void comparePayRunOptions(ComparePayRunOptionsRequest request, StreamObserver<ComparePayRunOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.comparePayRunOptions(request));
    }

    /**
     * Simulates a pay run.
     */
    @Override
    public void simulatePayRun(SimulatePayRunRequest request, StreamObserver<SimulatePayRunResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulatePayRun(request));
    }

    /**
     * Explains a pay run.
     */
    @Override
    public void explainPayRunCalculation(ExplainPayRunCalculationRequest request, StreamObserver<ExplainPayRunCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainPayRunCalculation(request));
    }

    /**
     * Returns a pay run.
     */
    @Override
    public void getPayRunRates(GetPayRunRatesRequest request, StreamObserver<GetPayRunRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getPayRunRates(request));
    }
}
