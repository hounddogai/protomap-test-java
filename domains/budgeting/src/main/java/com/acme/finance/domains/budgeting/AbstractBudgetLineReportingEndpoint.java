package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.BudgetLineReportingServiceGrpc;
import com.acme.budgeting.v1.CountBudgetLineTotalsRequest;
import com.acme.budgeting.v1.CountBudgetLineTotalsResponse;
import com.acme.budgeting.v1.DeleteBudgetLineReportRequest;
import com.acme.budgeting.v1.DeleteBudgetLineReportResponse;
import com.acme.budgeting.v1.FetchBudgetLineReportRequest;
import com.acme.budgeting.v1.FetchBudgetLineReportResponse;
import com.acme.budgeting.v1.GenerateBudgetLineReportRequest;
import com.acme.budgeting.v1.GenerateBudgetLineReportResponse;
import com.acme.budgeting.v1.ListBudgetLineReportsRequest;
import com.acme.budgeting.v1.ListBudgetLineReportsResponse;
import com.acme.budgeting.v1.ScheduleBudgetLineReportRequest;
import com.acme.budgeting.v1.ScheduleBudgetLineReportResponse;
import com.acme.budgeting.v1.ShareBudgetLineReportRequest;
import com.acme.budgeting.v1.ShareBudgetLineReportResponse;
import com.acme.budgeting.v1.StreamBudgetLineMetricsRequest;
import com.acme.budgeting.v1.StreamBudgetLineMetricsResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Builds and schedules reports about budget lines.
 */
public abstract class AbstractBudgetLineReportingEndpoint extends BudgetLineReportingServiceGrpc.BudgetLineReportingServiceImplBase {
    protected final BudgetLineReportingApplication application;

    protected AbstractBudgetLineReportingEndpoint(BudgetLineReportingApplication application) {
        this.application = application;
    }

    /**
     * Generates a budget line.
     */
    @Override
    public void generateBudgetLineReport(GenerateBudgetLineReportRequest request, StreamObserver<GenerateBudgetLineReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.generateBudgetLineReport(request));
    }

    /**
     * Returns a budget line.
     */
    @Override
    public void fetchBudgetLineReport(FetchBudgetLineReportRequest request, StreamObserver<FetchBudgetLineReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.fetchBudgetLineReport(request));
    }

    /**
     * Schedules a budget line.
     */
    @Override
    public void scheduleBudgetLineReport(ScheduleBudgetLineReportRequest request, StreamObserver<ScheduleBudgetLineReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleBudgetLineReport(request));
    }

    /**
     * Streams the changes of budget lines as they happen.
     */
    @Override
    public void streamBudgetLineMetrics(StreamBudgetLineMetricsRequest request, StreamObserver<StreamBudgetLineMetricsResponse> observer) {
        try {
            var snapshot = application.streamBudgetLineMetrics(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Lists budget lines a page at a time.
     */
    @Override
    public void listBudgetLineReports(ListBudgetLineReportsRequest request, StreamObserver<ListBudgetLineReportsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listBudgetLineReports(request));
    }

    /**
     * Shares a budget line.
     */
    @Override
    public void shareBudgetLineReport(ShareBudgetLineReportRequest request, StreamObserver<ShareBudgetLineReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.shareBudgetLineReport(request));
    }

    /**
     * Counts budget lines by status.
     */
    @Override
    public void countBudgetLineTotals(CountBudgetLineTotalsRequest request, StreamObserver<CountBudgetLineTotalsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countBudgetLineTotals(request));
    }

    /**
     * Deletes a budget line.
     */
    @Override
    public void deleteBudgetLineReport(DeleteBudgetLineReportRequest request, StreamObserver<DeleteBudgetLineReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.deleteBudgetLineReport(request));
    }
}
