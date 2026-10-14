package com.acme.finance.domains.treasury;

import com.acme.finance.platform.RpcSupport;
import com.acme.treasury.v1.CancelLiquidityForecastImportRequest;
import com.acme.treasury.v1.CancelLiquidityForecastImportResponse;
import com.acme.treasury.v1.GetLiquidityForecastImportStatusRequest;
import com.acme.treasury.v1.GetLiquidityForecastImportStatusResponse;
import com.acme.treasury.v1.ImportLiquidityForecastsRequest;
import com.acme.treasury.v1.ImportLiquidityForecastsResponse;
import com.acme.treasury.v1.LiquidityForecastImportServiceGrpc;
import com.acme.treasury.v1.ListLiquidityForecastImportsRequest;
import com.acme.treasury.v1.ListLiquidityForecastImportsResponse;
import com.acme.treasury.v1.MapLiquidityForecastImportColumnsRequest;
import com.acme.treasury.v1.MapLiquidityForecastImportColumnsResponse;
import com.acme.treasury.v1.RetryLiquidityForecastImportRequest;
import com.acme.treasury.v1.RetryLiquidityForecastImportResponse;
import com.acme.treasury.v1.ValidateLiquidityForecastImportRequest;
import com.acme.treasury.v1.ValidateLiquidityForecastImportResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Takes liquidity forecasts in bulk from partners and legacy systems.
 */
@GrpcService
public class LiquidityForecastImportEndpoint extends LiquidityForecastImportServiceGrpc.LiquidityForecastImportServiceImplBase {
    protected final LiquidityForecastImportApplication application;

    public LiquidityForecastImportEndpoint(LiquidityForecastImportApplication application) {
        this.application = application;
    }

    /**
     * Takes a stream of liquidity forecasts and answers once at the end.
     */
    @Override
    public StreamObserver<ImportLiquidityForecastsRequest> importLiquidityForecasts(StreamObserver<ImportLiquidityForecastsResponse> observer) {
        return new StreamObserver<ImportLiquidityForecastsRequest>() {
            private boolean finished;
            private final ImportLiquidityForecastsResponse.Builder summary = ImportLiquidityForecastsResponse.newBuilder();

            @Override
            public void onNext(ImportLiquidityForecastsRequest request) {
                if (finished) { return; }
                try {
                    var response = application.importLiquidityForecasts(request);
                    summary.mergeFrom(response);
                } catch (RuntimeException failure) {
                    finished = true;
                    observer.onError(RpcSupport.status(failure));
                }
            }

            @Override
            public void onError(Throwable failure) {
                if (!finished) {
                    finished = true;
                    observer.onError(RpcSupport.status(failure));
                }
            }

            @Override
            public void onCompleted() {
                if (finished) { return; }
                finished = true;
                observer.onNext(summary.build());
                observer.onCompleted();
            }
        };
    }

    /**
     * Returns a liquidity forecast.
     */
    @Override
    public void getLiquidityForecastImportStatus(GetLiquidityForecastImportStatusRequest request, StreamObserver<GetLiquidityForecastImportStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getLiquidityForecastImportStatus(request));
    }

    /**
     * Cancels a liquidity forecast.
     */
    @Override
    public void cancelLiquidityForecastImport(CancelLiquidityForecastImportRequest request, StreamObserver<CancelLiquidityForecastImportResponse> observer) {
        RpcSupport.unary(observer, () -> application.cancelLiquidityForecastImport(request));
    }

    /**
     * Validates a liquidity forecast.
     */
    @Override
    public void validateLiquidityForecastImport(ValidateLiquidityForecastImportRequest request, StreamObserver<ValidateLiquidityForecastImportResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateLiquidityForecastImport(request));
    }

    /**
     * Lists liquidity forecasts a page at a time.
     */
    @Override
    public void listLiquidityForecastImports(ListLiquidityForecastImportsRequest request, StreamObserver<ListLiquidityForecastImportsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listLiquidityForecastImports(request));
    }

    /**
     * Retrys a liquidity forecast.
     */
    @Override
    public void retryLiquidityForecastImport(RetryLiquidityForecastImportRequest request, StreamObserver<RetryLiquidityForecastImportResponse> observer) {
        RpcSupport.unary(observer, () -> application.retryLiquidityForecastImport(request));
    }

    /**
     * Maps a liquidity forecast.
     */
    @Override
    public void mapLiquidityForecastImportColumns(MapLiquidityForecastImportColumnsRequest request, StreamObserver<MapLiquidityForecastImportColumnsResponse> observer) {
        RpcSupport.unary(observer, () -> application.mapLiquidityForecastImportColumns(request));
    }
}
