package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.CountVarianceTotalsRequest;
import com.acme.budgeting.v1.CountVarianceTotalsResponse;
import com.acme.budgeting.v1.DeleteVarianceReportRequest;
import com.acme.budgeting.v1.DeleteVarianceReportResponse;
import com.acme.budgeting.v1.FetchVarianceReportRequest;
import com.acme.budgeting.v1.FetchVarianceReportResponse;
import com.acme.budgeting.v1.GenerateVarianceReportRequest;
import com.acme.budgeting.v1.GenerateVarianceReportResponse;
import com.acme.budgeting.v1.ListVarianceReportsRequest;
import com.acme.budgeting.v1.ListVarianceReportsResponse;
import com.acme.budgeting.v1.ScheduleVarianceReportRequest;
import com.acme.budgeting.v1.ScheduleVarianceReportResponse;
import com.acme.budgeting.v1.ShareVarianceReportRequest;
import com.acme.budgeting.v1.ShareVarianceReportResponse;
import com.acme.budgeting.v1.StreamVarianceMetricsRequest;
import com.acme.budgeting.v1.StreamVarianceMetricsResponse;
import com.acme.budgeting.v1.VarianceReportingServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Builds and schedules reports about variances.
 */
@GrpcService
public class VarianceReportingEndpoint extends VarianceReportingServiceGrpc.VarianceReportingServiceImplBase {
    protected final VarianceReportingApplication application;

    public VarianceReportingEndpoint(VarianceReportingApplication application) {
        this.application = application;
    }

    /**
     * Generates a variance.
     */
    @Override
    public void generateVarianceReport(GenerateVarianceReportRequest request, StreamObserver<GenerateVarianceReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.generateVarianceReport(request));
    }

    /**
     * Returns a variance.
     */
    @Override
    public void fetchVarianceReport(FetchVarianceReportRequest request, StreamObserver<FetchVarianceReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.fetchVarianceReport(request));
    }

    /**
     * Schedules a variance.
     */
    @Override
    public void scheduleVarianceReport(ScheduleVarianceReportRequest request, StreamObserver<ScheduleVarianceReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleVarianceReport(request));
    }

    /**
     * Streams the changes of variances as they happen.
     */
    @Override
    public void streamVarianceMetrics(StreamVarianceMetricsRequest request, StreamObserver<StreamVarianceMetricsResponse> observer) {
        try {
            var snapshot = application.streamVarianceMetrics(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Lists variances a page at a time.
     */
    @Override
    public void listVarianceReports(ListVarianceReportsRequest request, StreamObserver<ListVarianceReportsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listVarianceReports(request));
    }

    /**
     * Shares a variance.
     */
    @Override
    public void shareVarianceReport(ShareVarianceReportRequest request, StreamObserver<ShareVarianceReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.shareVarianceReport(request));
    }

    /**
     * Counts variances by status.
     */
    @Override
    public void countVarianceTotals(CountVarianceTotalsRequest request, StreamObserver<CountVarianceTotalsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countVarianceTotals(request));
    }

    /**
     * Deletes a variance.
     */
    @Override
    public void deleteVarianceReport(DeleteVarianceReportRequest request, StreamObserver<DeleteVarianceReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.deleteVarianceReport(request));
    }
}
