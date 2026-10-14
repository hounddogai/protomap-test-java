package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.ActivateForecastRequest;
import com.acme.budgeting.v1.ActivateForecastResponse;
import com.acme.budgeting.v1.ExpireForecastRequest;
import com.acme.budgeting.v1.ExpireForecastResponse;
import com.acme.budgeting.v1.ForecastLifecycleServiceGrpc;
import com.acme.budgeting.v1.GetForecastLifecycleRequest;
import com.acme.budgeting.v1.GetForecastLifecycleResponse;
import com.acme.budgeting.v1.ReinstateForecastRequest;
import com.acme.budgeting.v1.ReinstateForecastResponse;
import com.acme.budgeting.v1.RenewForecastRequest;
import com.acme.budgeting.v1.RenewForecastResponse;
import com.acme.budgeting.v1.ScheduleForecastExpiryRequest;
import com.acme.budgeting.v1.ScheduleForecastExpiryResponse;
import com.acme.budgeting.v1.SuspendForecastRequest;
import com.acme.budgeting.v1.SuspendForecastResponse;
import com.acme.budgeting.v1.TerminateForecastRequest;
import com.acme.budgeting.v1.TerminateForecastResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Moves a forecast between the states of its lifecycle.
 */
@GrpcService
public class ForecastLifecycleEndpoint extends ForecastLifecycleServiceGrpc.ForecastLifecycleServiceImplBase {
    protected final ForecastLifecycleApplication application;

    public ForecastLifecycleEndpoint(ForecastLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates a forecast.
     */
    @Override
    public void activateForecast(ActivateForecastRequest request, StreamObserver<ActivateForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.activateForecast(request));
    }

    /**
     * Suspends a forecast.
     */
    @Override
    public void suspendForecast(SuspendForecastRequest request, StreamObserver<SuspendForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendForecast(request));
    }

    /**
     * Reinstates a forecast.
     */
    @Override
    public void reinstateForecast(ReinstateForecastRequest request, StreamObserver<ReinstateForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.reinstateForecast(request));
    }

    /**
     * Terminates a forecast.
     */
    @Override
    public void terminateForecast(TerminateForecastRequest request, StreamObserver<TerminateForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.terminateForecast(request));
    }

    /**
     * Renews a forecast.
     */
    @Override
    public void renewForecast(RenewForecastRequest request, StreamObserver<RenewForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.renewForecast(request));
    }

    /**
     * Expires a forecast.
     */
    @Override
    public void expireForecast(ExpireForecastRequest request, StreamObserver<ExpireForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.expireForecast(request));
    }

    /**
     * Returns a forecast.
     */
    @Override
    public void getForecastLifecycle(GetForecastLifecycleRequest request, StreamObserver<GetForecastLifecycleResponse> observer) {
        RpcSupport.unary(observer, () -> application.getForecastLifecycle(request));
    }

    /**
     * Schedules a forecast.
     */
    @Override
    public void scheduleForecastExpiry(ScheduleForecastExpiryRequest request, StreamObserver<ScheduleForecastExpiryResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleForecastExpiry(request));
    }
}
