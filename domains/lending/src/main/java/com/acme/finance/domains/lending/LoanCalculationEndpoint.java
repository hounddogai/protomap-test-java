package com.acme.finance.domains.lending;

import com.acme.finance.platform.RpcSupport;
import com.acme.lending.v1.CalculateLoanRequest;
import com.acme.lending.v1.CalculateLoanResponse;
import com.acme.lending.v1.CompareLoanOptionsRequest;
import com.acme.lending.v1.CompareLoanOptionsResponse;
import com.acme.lending.v1.EstimateLoanRequest;
import com.acme.lending.v1.EstimateLoanResponse;
import com.acme.lending.v1.ExplainLoanCalculationRequest;
import com.acme.lending.v1.ExplainLoanCalculationResponse;
import com.acme.lending.v1.GetLoanRatesRequest;
import com.acme.lending.v1.GetLoanRatesResponse;
import com.acme.lending.v1.LoanCalculationServiceGrpc;
import com.acme.lending.v1.QuoteLoanRequest;
import com.acme.lending.v1.QuoteLoanResponse;
import com.acme.lending.v1.RecalculateLoansRequest;
import com.acme.lending.v1.RecalculateLoansResponse;
import com.acme.lending.v1.SimulateLoanRequest;
import com.acme.lending.v1.SimulateLoanResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of a loan, such as totals, taxes, and estimates.
 */
@GrpcService
public class LoanCalculationEndpoint extends LoanCalculationServiceGrpc.LoanCalculationServiceImplBase {
    protected final LoanCalculationApplication application;

    public LoanCalculationEndpoint(LoanCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a loan.
     */
    @Override
    public void calculateLoan(CalculateLoanRequest request, StreamObserver<CalculateLoanResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateLoan(request));
    }

    /**
     * Estimates a loan.
     */
    @Override
    public void estimateLoan(EstimateLoanRequest request, StreamObserver<EstimateLoanResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateLoan(request));
    }

    /**
     * Quotes a loan.
     */
    @Override
    public void quoteLoan(QuoteLoanRequest request, StreamObserver<QuoteLoanResponse> observer) {
        RpcSupport.unary(observer, () -> application.quoteLoan(request));
    }

    /**
     * Compares a loan.
     */
    @Override
    public void compareLoanOptions(CompareLoanOptionsRequest request, StreamObserver<CompareLoanOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareLoanOptions(request));
    }

    /**
     * Recalculates a loan.
     */
    @Override
    public void recalculateLoans(RecalculateLoansRequest request, StreamObserver<RecalculateLoansResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateLoans(request));
    }

    /**
     * Simulates a loan.
     */
    @Override
    public void simulateLoan(SimulateLoanRequest request, StreamObserver<SimulateLoanResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateLoan(request));
    }

    /**
     * Explains a loan.
     */
    @Override
    public void explainLoanCalculation(ExplainLoanCalculationRequest request, StreamObserver<ExplainLoanCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainLoanCalculation(request));
    }

    /**
     * Returns a loan.
     */
    @Override
    public void getLoanRates(GetLoanRatesRequest request, StreamObserver<GetLoanRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getLoanRates(request));
    }
}
