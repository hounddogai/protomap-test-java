package com.acme.finance.domains.treasury;

import com.acme.finance.platform.RpcSupport;
import com.acme.treasury.v1.CountLiquidityForecastTotalsRequest;
import com.acme.treasury.v1.CountLiquidityForecastTotalsResponse;
import com.acme.treasury.v1.DeleteLiquidityForecastReportRequest;
import com.acme.treasury.v1.DeleteLiquidityForecastReportResponse;
import com.acme.treasury.v1.FetchLiquidityForecastReportRequest;
import com.acme.treasury.v1.FetchLiquidityForecastReportResponse;
import com.acme.treasury.v1.GenerateLiquidityForecastReportRequest;
import com.acme.treasury.v1.GenerateLiquidityForecastReportResponse;
import com.acme.treasury.v1.LiquidityForecastReportingServiceGrpc;
import com.acme.treasury.v1.ScheduleLiquidityForecastReportRequest;
import com.acme.treasury.v1.ScheduleLiquidityForecastReportResponse;
import com.acme.treasury.v1.ShareLiquidityForecastReportRequest;
import com.acme.treasury.v1.ShareLiquidityForecastReportResponse;
import com.acme.treasury.v1.StreamLiquidityForecastMetricsRequest;
import com.acme.treasury.v1.StreamLiquidityForecastMetricsResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Builds and schedules reports about liquidity forecasts.
 */
@GrpcService
public class LiquidityForecastReportingEndpoint extends LiquidityForecastReportingServiceGrpc.LiquidityForecastReportingServiceImplBase {
    protected final LiquidityForecastReportingApplication application;

    public LiquidityForecastReportingEndpoint(LiquidityForecastReportingApplication application) {
        this.application = application;
    }

    /**
     * Generates a liquidity forecast.
     */
    @Override
    public void generateLiquidityForecastReport(GenerateLiquidityForecastReportRequest request, StreamObserver<GenerateLiquidityForecastReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.generateLiquidityForecastReport(request));
    }

    /**
     * Returns a liquidity forecast.
     */
    @Override
    public void fetchLiquidityForecastReport(FetchLiquidityForecastReportRequest request, StreamObserver<FetchLiquidityForecastReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.fetchLiquidityForecastReport(request));
    }

    /**
     * Schedules a liquidity forecast.
     */
    @Override
    public void scheduleLiquidityForecastReport(ScheduleLiquidityForecastReportRequest request, StreamObserver<ScheduleLiquidityForecastReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleLiquidityForecastReport(request));
    }

    /**
     * Streams the changes of liquidity forecasts as they happen.
     */
    @Override
    public void streamLiquidityForecastMetrics(StreamLiquidityForecastMetricsRequest request, StreamObserver<StreamLiquidityForecastMetricsResponse> observer) {
        try {
            var snapshot = application.streamLiquidityForecastMetrics(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Shares a liquidity forecast.
     */
    @Override
    public void shareLiquidityForecastReport(ShareLiquidityForecastReportRequest request, StreamObserver<ShareLiquidityForecastReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.shareLiquidityForecastReport(request));
    }

    /**
     * Counts liquidity forecasts by status.
     */
    @Override
    public void countLiquidityForecastTotals(CountLiquidityForecastTotalsRequest request, StreamObserver<CountLiquidityForecastTotalsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countLiquidityForecastTotals(request));
    }

    /**
     * Deletes a liquidity forecast.
     */
    @Override
    public void deleteLiquidityForecastReport(DeleteLiquidityForecastReportRequest request, StreamObserver<DeleteLiquidityForecastReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.deleteLiquidityForecastReport(request));
    }
}
