package com.acme.finance.domains.payroll;

import com.acme.finance.platform.RpcSupport;
import com.acme.payroll.v1.CountDeductionTotalsRequest;
import com.acme.payroll.v1.CountDeductionTotalsResponse;
import com.acme.payroll.v1.DeductionReportingServiceGrpc;
import com.acme.payroll.v1.DeleteDeductionReportRequest;
import com.acme.payroll.v1.DeleteDeductionReportResponse;
import com.acme.payroll.v1.FetchDeductionReportRequest;
import com.acme.payroll.v1.FetchDeductionReportResponse;
import com.acme.payroll.v1.GenerateDeductionReportRequest;
import com.acme.payroll.v1.GenerateDeductionReportResponse;
import com.acme.payroll.v1.ListDeductionReportsRequest;
import com.acme.payroll.v1.ListDeductionReportsResponse;
import com.acme.payroll.v1.ScheduleDeductionReportRequest;
import com.acme.payroll.v1.ScheduleDeductionReportResponse;
import com.acme.payroll.v1.ShareDeductionReportRequest;
import com.acme.payroll.v1.ShareDeductionReportResponse;
import com.acme.payroll.v1.StreamDeductionMetricsRequest;
import com.acme.payroll.v1.StreamDeductionMetricsResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Builds and schedules reports about deductions.
 */
@GrpcService
public class DeductionReportingEndpoint extends DeductionReportingServiceGrpc.DeductionReportingServiceImplBase {
    protected final DeductionReportingApplication application;

    public DeductionReportingEndpoint(DeductionReportingApplication application) {
        this.application = application;
    }

    /**
     * Generates a deduction.
     */
    @Override
    public void generateDeductionReport(GenerateDeductionReportRequest request, StreamObserver<GenerateDeductionReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.generateDeductionReport(request));
    }

    /**
     * Returns a deduction.
     */
    @Override
    public void fetchDeductionReport(FetchDeductionReportRequest request, StreamObserver<FetchDeductionReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.fetchDeductionReport(request));
    }

    /**
     * Schedules a deduction.
     */
    @Override
    public void scheduleDeductionReport(ScheduleDeductionReportRequest request, StreamObserver<ScheduleDeductionReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleDeductionReport(request));
    }

    /**
     * Streams the changes of deductions as they happen.
     */
    @Override
    public void streamDeductionMetrics(StreamDeductionMetricsRequest request, StreamObserver<StreamDeductionMetricsResponse> observer) {
        try {
            var snapshot = application.streamDeductionMetrics(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Lists deductions a page at a time.
     */
    @Override
    public void listDeductionReports(ListDeductionReportsRequest request, StreamObserver<ListDeductionReportsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listDeductionReports(request));
    }

    /**
     * Shares a deduction.
     */
    @Override
    public void shareDeductionReport(ShareDeductionReportRequest request, StreamObserver<ShareDeductionReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.shareDeductionReport(request));
    }

    /**
     * Counts deductions by status.
     */
    @Override
    public void countDeductionTotals(CountDeductionTotalsRequest request, StreamObserver<CountDeductionTotalsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countDeductionTotals(request));
    }

    /**
     * Deletes a deduction.
     */
    @Override
    public void deleteDeductionReport(DeleteDeductionReportRequest request, StreamObserver<DeleteDeductionReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.deleteDeductionReport(request));
    }
}
