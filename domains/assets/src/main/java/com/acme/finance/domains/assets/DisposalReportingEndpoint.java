package com.acme.finance.domains.assets;

import com.acme.assets.v1.CountDisposalTotalsRequest;
import com.acme.assets.v1.CountDisposalTotalsResponse;
import com.acme.assets.v1.DeleteDisposalReportRequest;
import com.acme.assets.v1.DeleteDisposalReportResponse;
import com.acme.assets.v1.DisposalReportingServiceGrpc;
import com.acme.assets.v1.FetchDisposalReportRequest;
import com.acme.assets.v1.FetchDisposalReportResponse;
import com.acme.assets.v1.GenerateDisposalReportRequest;
import com.acme.assets.v1.GenerateDisposalReportResponse;
import com.acme.assets.v1.ListDisposalReportsRequest;
import com.acme.assets.v1.ListDisposalReportsResponse;
import com.acme.assets.v1.ScheduleDisposalReportRequest;
import com.acme.assets.v1.ScheduleDisposalReportResponse;
import com.acme.assets.v1.ShareDisposalReportRequest;
import com.acme.assets.v1.ShareDisposalReportResponse;
import com.acme.assets.v1.StreamDisposalMetricsRequest;
import com.acme.assets.v1.StreamDisposalMetricsResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Builds and schedules reports about disposals.
 */
@GrpcService
public class DisposalReportingEndpoint extends DisposalReportingServiceGrpc.DisposalReportingServiceImplBase {
    protected final DisposalReportingApplication application;

    public DisposalReportingEndpoint(DisposalReportingApplication application) {
        this.application = application;
    }

    /**
     * Generates a disposal.
     */
    @Override
    public void generateDisposalReport(GenerateDisposalReportRequest request, StreamObserver<GenerateDisposalReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.generateDisposalReport(request));
    }

    /**
     * Returns a disposal.
     */
    @Override
    public void fetchDisposalReport(FetchDisposalReportRequest request, StreamObserver<FetchDisposalReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.fetchDisposalReport(request));
    }

    /**
     * Schedules a disposal.
     */
    @Override
    public void scheduleDisposalReport(ScheduleDisposalReportRequest request, StreamObserver<ScheduleDisposalReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleDisposalReport(request));
    }

    /**
     * Streams the changes of disposals as they happen.
     */
    @Override
    public void streamDisposalMetrics(StreamDisposalMetricsRequest request, StreamObserver<StreamDisposalMetricsResponse> observer) {
        try {
            var snapshot = application.streamDisposalMetrics(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Lists disposals a page at a time.
     */
    @Override
    public void listDisposalReports(ListDisposalReportsRequest request, StreamObserver<ListDisposalReportsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listDisposalReports(request));
    }

    /**
     * Shares a disposal.
     */
    @Override
    public void shareDisposalReport(ShareDisposalReportRequest request, StreamObserver<ShareDisposalReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.shareDisposalReport(request));
    }

    /**
     * Counts disposals by status.
     */
    @Override
    public void countDisposalTotals(CountDisposalTotalsRequest request, StreamObserver<CountDisposalTotalsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countDisposalTotals(request));
    }

    /**
     * Deletes a disposal.
     */
    @Override
    public void deleteDisposalReport(DeleteDisposalReportRequest request, StreamObserver<DeleteDisposalReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.deleteDisposalReport(request));
    }
}
