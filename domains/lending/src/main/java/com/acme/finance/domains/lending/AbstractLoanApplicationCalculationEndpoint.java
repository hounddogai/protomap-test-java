package com.acme.finance.domains.lending;

import com.acme.finance.platform.RpcSupport;
import com.acme.lending.v1.CalculateLoanApplicationRequest;
import com.acme.lending.v1.CalculateLoanApplicationResponse;
import com.acme.lending.v1.CompareLoanApplicationOptionsRequest;
import com.acme.lending.v1.CompareLoanApplicationOptionsResponse;
import com.acme.lending.v1.EstimateLoanApplicationRequest;
import com.acme.lending.v1.EstimateLoanApplicationResponse;
import com.acme.lending.v1.ExplainLoanApplicationCalculationRequest;
import com.acme.lending.v1.ExplainLoanApplicationCalculationResponse;
import com.acme.lending.v1.GetLoanApplicationRatesRequest;
import com.acme.lending.v1.GetLoanApplicationRatesResponse;
import com.acme.lending.v1.LoanApplicationCalculationServiceGrpc;
import com.acme.lending.v1.QuoteLoanApplicationRequest;
import com.acme.lending.v1.QuoteLoanApplicationResponse;
import com.acme.lending.v1.RecalculateLoanApplicationsRequest;
import com.acme.lending.v1.RecalculateLoanApplicationsResponse;
import com.acme.lending.v1.SimulateLoanApplicationRequest;
import com.acme.lending.v1.SimulateLoanApplicationResponse;
import io.grpc.stub.StreamObserver;

/**
 * Computes the figures of a loan application, such as totals, taxes, and estimates.
 */
public abstract class AbstractLoanApplicationCalculationEndpoint extends LoanApplicationCalculationServiceGrpc.LoanApplicationCalculationServiceImplBase {
    protected final LoanApplicationCalculationApplication application;

    protected AbstractLoanApplicationCalculationEndpoint(LoanApplicationCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a loan application.
     */
    @Override
    public void calculateLoanApplication(CalculateLoanApplicationRequest request, StreamObserver<CalculateLoanApplicationResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateLoanApplication(request));
    }

    /**
     * Estimates a loan application.
     */
    @Override
    public void estimateLoanApplication(EstimateLoanApplicationRequest request, StreamObserver<EstimateLoanApplicationResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateLoanApplication(request));
    }

    /**
     * Quotes a loan application.
     */
    @Override
    public void quoteLoanApplication(QuoteLoanApplicationRequest request, StreamObserver<QuoteLoanApplicationResponse> observer) {
        RpcSupport.unary(observer, () -> application.quoteLoanApplication(request));
    }

    /**
     * Compares a loan application.
     */
    @Override
    public void compareLoanApplicationOptions(CompareLoanApplicationOptionsRequest request, StreamObserver<CompareLoanApplicationOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareLoanApplicationOptions(request));
    }

    /**
     * Recalculates a loan application.
     */
    @Override
    public void recalculateLoanApplications(RecalculateLoanApplicationsRequest request, StreamObserver<RecalculateLoanApplicationsResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateLoanApplications(request));
    }

    /**
     * Simulates a loan application.
     */
    @Override
    public void simulateLoanApplication(SimulateLoanApplicationRequest request, StreamObserver<SimulateLoanApplicationResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateLoanApplication(request));
    }

    /**
     * Explains a loan application.
     */
    @Override
    public void explainLoanApplicationCalculation(ExplainLoanApplicationCalculationRequest request, StreamObserver<ExplainLoanApplicationCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainLoanApplicationCalculation(request));
    }

    /**
     * Returns a loan application.
     */
    @Override
    public void getLoanApplicationRates(GetLoanApplicationRatesRequest request, StreamObserver<GetLoanApplicationRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getLoanApplicationRates(request));
    }
}
