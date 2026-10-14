package com.acme.finance.domains.expenses;

import com.acme.expenses.v1.CountExpensePolicyTotalsRequest;
import com.acme.expenses.v1.CountExpensePolicyTotalsResponse;
import com.acme.expenses.v1.DeleteExpensePolicyReportRequest;
import com.acme.expenses.v1.DeleteExpensePolicyReportResponse;
import com.acme.expenses.v1.ExpensePolicyReportingServiceGrpc;
import com.acme.expenses.v1.FetchExpensePolicyReportRequest;
import com.acme.expenses.v1.FetchExpensePolicyReportResponse;
import com.acme.expenses.v1.GenerateExpensePolicyReportRequest;
import com.acme.expenses.v1.GenerateExpensePolicyReportResponse;
import com.acme.expenses.v1.ListExpensePolicyReportsRequest;
import com.acme.expenses.v1.ListExpensePolicyReportsResponse;
import com.acme.expenses.v1.ScheduleExpensePolicyReportRequest;
import com.acme.expenses.v1.ScheduleExpensePolicyReportResponse;
import com.acme.expenses.v1.StreamExpensePolicyMetricsRequest;
import com.acme.expenses.v1.StreamExpensePolicyMetricsResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Builds and schedules reports about expense policies.
 */
@GrpcService
public class ExpensePolicyReportingEndpoint extends ExpensePolicyReportingServiceGrpc.ExpensePolicyReportingServiceImplBase {
    protected final ExpensePolicyReportingApplication application;

    public ExpensePolicyReportingEndpoint(ExpensePolicyReportingApplication application) {
        this.application = application;
    }

    /**
     * Generates an expense policy.
     */
    @Override
    public void generateExpensePolicyReport(GenerateExpensePolicyReportRequest request, StreamObserver<GenerateExpensePolicyReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.generateExpensePolicyReport(request));
    }

    /**
     * Returns an expense policy.
     */
    @Override
    public void fetchExpensePolicyReport(FetchExpensePolicyReportRequest request, StreamObserver<FetchExpensePolicyReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.fetchExpensePolicyReport(request));
    }

    /**
     * Schedules an expense policy.
     */
    @Override
    public void scheduleExpensePolicyReport(ScheduleExpensePolicyReportRequest request, StreamObserver<ScheduleExpensePolicyReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleExpensePolicyReport(request));
    }

    /**
     * Streams the changes of expense policies as they happen.
     */
    @Override
    public void streamExpensePolicyMetrics(StreamExpensePolicyMetricsRequest request, StreamObserver<StreamExpensePolicyMetricsResponse> observer) {
        try {
            var snapshot = application.streamExpensePolicyMetrics(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Lists expense policies a page at a time.
     */
    @Override
    public void listExpensePolicyReports(ListExpensePolicyReportsRequest request, StreamObserver<ListExpensePolicyReportsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listExpensePolicyReports(request));
    }

    /**
     * Counts expense policies by status.
     */
    @Override
    public void countExpensePolicyTotals(CountExpensePolicyTotalsRequest request, StreamObserver<CountExpensePolicyTotalsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countExpensePolicyTotals(request));
    }

    /**
     * Deletes an expense policy.
     */
    @Override
    public void deleteExpensePolicyReport(DeleteExpensePolicyReportRequest request, StreamObserver<DeleteExpensePolicyReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.deleteExpensePolicyReport(request));
    }
}
