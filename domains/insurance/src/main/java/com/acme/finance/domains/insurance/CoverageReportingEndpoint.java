package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.CountCoverageTotalsRequest;
import com.acme.insurance.v1.CountCoverageTotalsResponse;
import com.acme.insurance.v1.CoverageReportingServiceGrpc;
import com.acme.insurance.v1.DeleteCoverageReportRequest;
import com.acme.insurance.v1.DeleteCoverageReportResponse;
import com.acme.insurance.v1.FetchCoverageReportRequest;
import com.acme.insurance.v1.FetchCoverageReportResponse;
import com.acme.insurance.v1.GenerateCoverageReportRequest;
import com.acme.insurance.v1.GenerateCoverageReportResponse;
import com.acme.insurance.v1.ListCoverageReportsRequest;
import com.acme.insurance.v1.ListCoverageReportsResponse;
import com.acme.insurance.v1.ScheduleCoverageReportRequest;
import com.acme.insurance.v1.ScheduleCoverageReportResponse;
import com.acme.insurance.v1.ShareCoverageReportRequest;
import com.acme.insurance.v1.ShareCoverageReportResponse;
import com.acme.insurance.v1.StreamCoverageMetricsRequest;
import com.acme.insurance.v1.StreamCoverageMetricsResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Builds and schedules reports about coverages.
 */
@GrpcService
public class CoverageReportingEndpoint extends CoverageReportingServiceGrpc.CoverageReportingServiceImplBase {
    protected final CoverageReportingApplication application;

    public CoverageReportingEndpoint(CoverageReportingApplication application) {
        this.application = application;
    }

    /**
     * Generates a coverage.
     */
    @Override
    public void generateCoverageReport(GenerateCoverageReportRequest request, StreamObserver<GenerateCoverageReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.generateCoverageReport(request));
    }

    /**
     * Returns a coverage.
     */
    @Override
    public void fetchCoverageReport(FetchCoverageReportRequest request, StreamObserver<FetchCoverageReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.fetchCoverageReport(request));
    }

    /**
     * Schedules a coverage.
     */
    @Override
    public void scheduleCoverageReport(ScheduleCoverageReportRequest request, StreamObserver<ScheduleCoverageReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleCoverageReport(request));
    }

    /**
     * Streams the changes of coverages as they happen.
     */
    @Override
    public void streamCoverageMetrics(StreamCoverageMetricsRequest request, StreamObserver<StreamCoverageMetricsResponse> observer) {
        try {
            var snapshot = application.streamCoverageMetrics(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Lists coverages a page at a time.
     */
    @Override
    public void listCoverageReports(ListCoverageReportsRequest request, StreamObserver<ListCoverageReportsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listCoverageReports(request));
    }

    /**
     * Shares a coverage.
     */
    @Override
    public void shareCoverageReport(ShareCoverageReportRequest request, StreamObserver<ShareCoverageReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.shareCoverageReport(request));
    }

    /**
     * Counts coverages by status.
     */
    @Override
    public void countCoverageTotals(CountCoverageTotalsRequest request, StreamObserver<CountCoverageTotalsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countCoverageTotals(request));
    }

    /**
     * Deletes a coverage.
     */
    @Override
    public void deleteCoverageReport(DeleteCoverageReportRequest request, StreamObserver<DeleteCoverageReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.deleteCoverageReport(request));
    }
}
