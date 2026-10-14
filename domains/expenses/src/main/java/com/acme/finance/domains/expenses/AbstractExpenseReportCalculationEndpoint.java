package com.acme.finance.domains.expenses;

import com.acme.expenses.v1.CalculateExpenseReportRequest;
import com.acme.expenses.v1.CalculateExpenseReportResponse;
import com.acme.expenses.v1.CompareExpenseReportOptionsRequest;
import com.acme.expenses.v1.CompareExpenseReportOptionsResponse;
import com.acme.expenses.v1.EstimateExpenseReportRequest;
import com.acme.expenses.v1.EstimateExpenseReportResponse;
import com.acme.expenses.v1.ExpenseReportCalculationServiceGrpc;
import com.acme.expenses.v1.ExplainExpenseReportCalculationRequest;
import com.acme.expenses.v1.ExplainExpenseReportCalculationResponse;
import com.acme.expenses.v1.GetExpenseReportRatesRequest;
import com.acme.expenses.v1.GetExpenseReportRatesResponse;
import com.acme.expenses.v1.QuoteExpenseReportRequest;
import com.acme.expenses.v1.QuoteExpenseReportResponse;
import com.acme.expenses.v1.RecalculateExpenseReportsRequest;
import com.acme.expenses.v1.RecalculateExpenseReportsResponse;
import com.acme.expenses.v1.SimulateExpenseReportRequest;
import com.acme.expenses.v1.SimulateExpenseReportResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Computes the figures of an expense report, such as totals, taxes, and estimates.
 */
public abstract class AbstractExpenseReportCalculationEndpoint extends ExpenseReportCalculationServiceGrpc.ExpenseReportCalculationServiceImplBase {
    protected final ExpenseReportCalculationApplication application;

    protected AbstractExpenseReportCalculationEndpoint(ExpenseReportCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates an expense report.
     */
    @Override
    public void calculateExpenseReport(CalculateExpenseReportRequest request, StreamObserver<CalculateExpenseReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateExpenseReport(request));
    }

    /**
     * Estimates an expense report.
     */
    @Override
    public void estimateExpenseReport(EstimateExpenseReportRequest request, StreamObserver<EstimateExpenseReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateExpenseReport(request));
    }

    /**
     * Quotes an expense report.
     */
    @Override
    public void quoteExpenseReport(QuoteExpenseReportRequest request, StreamObserver<QuoteExpenseReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.quoteExpenseReport(request));
    }

    /**
     * Compares an expense report.
     */
    @Override
    public void compareExpenseReportOptions(CompareExpenseReportOptionsRequest request, StreamObserver<CompareExpenseReportOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareExpenseReportOptions(request));
    }

    /**
     * Recalculates an expense report.
     */
    @Override
    public void recalculateExpenseReports(RecalculateExpenseReportsRequest request, StreamObserver<RecalculateExpenseReportsResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateExpenseReports(request));
    }

    /**
     * Simulates an expense report.
     */
    @Override
    public void simulateExpenseReport(SimulateExpenseReportRequest request, StreamObserver<SimulateExpenseReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateExpenseReport(request));
    }

    /**
     * Explains an expense report.
     */
    @Override
    public void explainExpenseReportCalculation(ExplainExpenseReportCalculationRequest request, StreamObserver<ExplainExpenseReportCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainExpenseReportCalculation(request));
    }

    /**
     * Returns an expense report.
     */
    @Override
    public void getExpenseReportRates(GetExpenseReportRatesRequest request, StreamObserver<GetExpenseReportRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getExpenseReportRates(request));
    }
}
