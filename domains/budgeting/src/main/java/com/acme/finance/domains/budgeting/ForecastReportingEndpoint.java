package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.CountForecastTotalsRequest;
import com.acme.budgeting.v1.CountForecastTotalsResponse;
import com.acme.budgeting.v1.FetchForecastReportRequest;
import com.acme.budgeting.v1.FetchForecastReportResponse;
import com.acme.budgeting.v1.ForecastReportingServiceGrpc;
import com.acme.budgeting.v1.GenerateForecastReportRequest;
import com.acme.budgeting.v1.GenerateForecastReportResponse;
import com.acme.budgeting.v1.ListForecastReportsRequest;
import com.acme.budgeting.v1.ListForecastReportsResponse;
import com.acme.budgeting.v1.ScheduleForecastReportRequest;
import com.acme.budgeting.v1.ScheduleForecastReportResponse;
import com.acme.budgeting.v1.ShareForecastReportRequest;
import com.acme.budgeting.v1.ShareForecastReportResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Builds and schedules reports about forecasts.
 */
@GrpcService
public class ForecastReportingEndpoint extends ForecastReportingServiceGrpc.ForecastReportingServiceImplBase {
    protected final ForecastReportingApplication application;

    public ForecastReportingEndpoint(ForecastReportingApplication application) {
        this.application = application;
    }

    /**
     * Generates a forecast.
     */
    @Override
    public void generateForecastReport(GenerateForecastReportRequest request, StreamObserver<GenerateForecastReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.generateForecastReport(request));
    }

    /**
     * Returns a forecast.
     */
    @Override
    public void fetchForecastReport(FetchForecastReportRequest request, StreamObserver<FetchForecastReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.fetchForecastReport(request));
    }

    /**
     * Schedules a forecast.
     */
    @Override
    public void scheduleForecastReport(ScheduleForecastReportRequest request, StreamObserver<ScheduleForecastReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleForecastReport(request));
    }

    /**
     * Lists forecasts a page at a time.
     */
    @Override
    public void listForecastReports(ListForecastReportsRequest request, StreamObserver<ListForecastReportsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listForecastReports(request));
    }

    /**
     * Shares a forecast.
     */
    @Override
    public void shareForecastReport(ShareForecastReportRequest request, StreamObserver<ShareForecastReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.shareForecastReport(request));
    }

    /**
     * Counts forecasts by status.
     */
    @Override
    public void countForecastTotals(CountForecastTotalsRequest request, StreamObserver<CountForecastTotalsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countForecastTotals(request));
    }
}
