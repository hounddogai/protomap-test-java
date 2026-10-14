package com.acme.finance.domains.treasury;

import com.acme.finance.platform.RpcSupport;
import com.acme.treasury.v1.CountFxHedgeTotalsRequest;
import com.acme.treasury.v1.CountFxHedgeTotalsResponse;
import com.acme.treasury.v1.DeleteFxHedgeReportRequest;
import com.acme.treasury.v1.DeleteFxHedgeReportResponse;
import com.acme.treasury.v1.FetchFxHedgeReportRequest;
import com.acme.treasury.v1.FetchFxHedgeReportResponse;
import com.acme.treasury.v1.FxHedgeReportingServiceGrpc;
import com.acme.treasury.v1.GenerateFxHedgeReportRequest;
import com.acme.treasury.v1.GenerateFxHedgeReportResponse;
import com.acme.treasury.v1.ListFxHedgeReportsRequest;
import com.acme.treasury.v1.ListFxHedgeReportsResponse;
import com.acme.treasury.v1.StreamFxHedgeMetricsRequest;
import com.acme.treasury.v1.StreamFxHedgeMetricsResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Builds and schedules reports about fx hedges.
 */
@GrpcService
public class FxHedgeReportingEndpoint extends FxHedgeReportingServiceGrpc.FxHedgeReportingServiceImplBase {
    protected final FxHedgeReportingApplication application;

    public FxHedgeReportingEndpoint(FxHedgeReportingApplication application) {
        this.application = application;
    }

    /**
     * Generates a fx hedge.
     */
    @Override
    public void generateFxHedgeReport(GenerateFxHedgeReportRequest request, StreamObserver<GenerateFxHedgeReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.generateFxHedgeReport(request));
    }

    /**
     * Returns a fx hedge.
     */
    @Override
    public void fetchFxHedgeReport(FetchFxHedgeReportRequest request, StreamObserver<FetchFxHedgeReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.fetchFxHedgeReport(request));
    }

    /**
     * Streams the changes of fx hedges as they happen.
     */
    @Override
    public void streamFxHedgeMetrics(StreamFxHedgeMetricsRequest request, StreamObserver<StreamFxHedgeMetricsResponse> observer) {
        try {
            var snapshot = application.streamFxHedgeMetrics(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Lists fx hedges a page at a time.
     */
    @Override
    public void listFxHedgeReports(ListFxHedgeReportsRequest request, StreamObserver<ListFxHedgeReportsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listFxHedgeReports(request));
    }

    /**
     * Counts fx hedges by status.
     */
    @Override
    public void countFxHedgeTotals(CountFxHedgeTotalsRequest request, StreamObserver<CountFxHedgeTotalsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countFxHedgeTotals(request));
    }

    /**
     * Deletes a fx hedge.
     */
    @Override
    public void deleteFxHedgeReport(DeleteFxHedgeReportRequest request, StreamObserver<DeleteFxHedgeReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.deleteFxHedgeReport(request));
    }
}
