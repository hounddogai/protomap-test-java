package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.CountPremiumTotalsRequest;
import com.acme.insurance.v1.CountPremiumTotalsResponse;
import com.acme.insurance.v1.DeletePremiumReportRequest;
import com.acme.insurance.v1.DeletePremiumReportResponse;
import com.acme.insurance.v1.FetchPremiumReportRequest;
import com.acme.insurance.v1.FetchPremiumReportResponse;
import com.acme.insurance.v1.GeneratePremiumReportRequest;
import com.acme.insurance.v1.GeneratePremiumReportResponse;
import com.acme.insurance.v1.ListPremiumReportsRequest;
import com.acme.insurance.v1.ListPremiumReportsResponse;
import com.acme.insurance.v1.PremiumReportingServiceGrpc;
import com.acme.insurance.v1.SchedulePremiumReportRequest;
import com.acme.insurance.v1.SchedulePremiumReportResponse;
import com.acme.insurance.v1.SharePremiumReportRequest;
import com.acme.insurance.v1.SharePremiumReportResponse;
import com.acme.insurance.v1.StreamPremiumMetricsRequest;
import com.acme.insurance.v1.StreamPremiumMetricsResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Builds and schedules reports about premiums.
 */
@GrpcService
public class PremiumReportingEndpoint extends PremiumReportingServiceGrpc.PremiumReportingServiceImplBase {
    protected final PremiumReportingApplication application;

    public PremiumReportingEndpoint(PremiumReportingApplication application) {
        this.application = application;
    }

    /**
     * Generates a premium.
     */
    @Override
    public void generatePremiumReport(GeneratePremiumReportRequest request, StreamObserver<GeneratePremiumReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.generatePremiumReport(request));
    }

    /**
     * Returns a premium.
     */
    @Override
    public void fetchPremiumReport(FetchPremiumReportRequest request, StreamObserver<FetchPremiumReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.fetchPremiumReport(request));
    }

    /**
     * Schedules a premium.
     */
    @Override
    public void schedulePremiumReport(SchedulePremiumReportRequest request, StreamObserver<SchedulePremiumReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.schedulePremiumReport(request));
    }

    /**
     * Streams the changes of premiums as they happen.
     */
    @Override
    public void streamPremiumMetrics(StreamPremiumMetricsRequest request, StreamObserver<StreamPremiumMetricsResponse> observer) {
        try {
            var snapshot = application.streamPremiumMetrics(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Lists premiums a page at a time.
     */
    @Override
    public void listPremiumReports(ListPremiumReportsRequest request, StreamObserver<ListPremiumReportsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPremiumReports(request));
    }

    /**
     * Shares a premium.
     */
    @Override
    public void sharePremiumReport(SharePremiumReportRequest request, StreamObserver<SharePremiumReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.sharePremiumReport(request));
    }

    /**
     * Counts premiums by status.
     */
    @Override
    public void countPremiumTotals(CountPremiumTotalsRequest request, StreamObserver<CountPremiumTotalsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countPremiumTotals(request));
    }

    /**
     * Deletes a premium.
     */
    @Override
    public void deletePremiumReport(DeletePremiumReportRequest request, StreamObserver<DeletePremiumReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.deletePremiumReport(request));
    }
}
