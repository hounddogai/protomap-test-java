package com.acme.finance.domains.treasury;

import com.acme.finance.platform.RpcSupport;
import com.acme.treasury.v1.ActivateLiquidityForecastRequest;
import com.acme.treasury.v1.ActivateLiquidityForecastResponse;
import com.acme.treasury.v1.ExpireLiquidityForecastRequest;
import com.acme.treasury.v1.ExpireLiquidityForecastResponse;
import com.acme.treasury.v1.GetLiquidityForecastLifecycleRequest;
import com.acme.treasury.v1.GetLiquidityForecastLifecycleResponse;
import com.acme.treasury.v1.LiquidityForecastLifecycleServiceGrpc;
import com.acme.treasury.v1.ReinstateLiquidityForecastRequest;
import com.acme.treasury.v1.ReinstateLiquidityForecastResponse;
import com.acme.treasury.v1.RenewLiquidityForecastRequest;
import com.acme.treasury.v1.RenewLiquidityForecastResponse;
import com.acme.treasury.v1.ScheduleLiquidityForecastExpiryRequest;
import com.acme.treasury.v1.ScheduleLiquidityForecastExpiryResponse;
import com.acme.treasury.v1.SuspendLiquidityForecastRequest;
import com.acme.treasury.v1.SuspendLiquidityForecastResponse;
import com.acme.treasury.v1.TerminateLiquidityForecastRequest;
import com.acme.treasury.v1.TerminateLiquidityForecastResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Moves a liquidity forecast between the states of its lifecycle.
 */
@GrpcService
public class LiquidityForecastLifecycleEndpoint extends LiquidityForecastLifecycleServiceGrpc.LiquidityForecastLifecycleServiceImplBase {
    protected final LiquidityForecastLifecycleApplication application;

    public LiquidityForecastLifecycleEndpoint(LiquidityForecastLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates a liquidity forecast.
     */
    @Override
    public void activateLiquidityForecast(ActivateLiquidityForecastRequest request, StreamObserver<ActivateLiquidityForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.activateLiquidityForecast(request));
    }

    /**
     * Suspends a liquidity forecast.
     */
    @Override
    public void suspendLiquidityForecast(SuspendLiquidityForecastRequest request, StreamObserver<SuspendLiquidityForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendLiquidityForecast(request));
    }

    /**
     * Reinstates a liquidity forecast.
     */
    @Override
    public void reinstateLiquidityForecast(ReinstateLiquidityForecastRequest request, StreamObserver<ReinstateLiquidityForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.reinstateLiquidityForecast(request));
    }

    /**
     * Terminates a liquidity forecast.
     */
    @Override
    public void terminateLiquidityForecast(TerminateLiquidityForecastRequest request, StreamObserver<TerminateLiquidityForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.terminateLiquidityForecast(request));
    }

    /**
     * Renews a liquidity forecast.
     */
    @Override
    public void renewLiquidityForecast(RenewLiquidityForecastRequest request, StreamObserver<RenewLiquidityForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.renewLiquidityForecast(request));
    }

    /**
     * Expires a liquidity forecast.
     */
    @Override
    public void expireLiquidityForecast(ExpireLiquidityForecastRequest request, StreamObserver<ExpireLiquidityForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.expireLiquidityForecast(request));
    }

    /**
     * Returns a liquidity forecast.
     */
    @Override
    public void getLiquidityForecastLifecycle(GetLiquidityForecastLifecycleRequest request, StreamObserver<GetLiquidityForecastLifecycleResponse> observer) {
        RpcSupport.unary(observer, () -> application.getLiquidityForecastLifecycle(request));
    }

    /**
     * Schedules a liquidity forecast.
     */
    @Override
    public void scheduleLiquidityForecastExpiry(ScheduleLiquidityForecastExpiryRequest request, StreamObserver<ScheduleLiquidityForecastExpiryResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleLiquidityForecastExpiry(request));
    }
}
