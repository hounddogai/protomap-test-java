package com.acme.finance.domains.treasury;

import com.acme.finance.platform.RpcSupport;
import com.acme.treasury.v1.ActivateCashPositionRequest;
import com.acme.treasury.v1.ActivateCashPositionResponse;
import com.acme.treasury.v1.CashPositionLifecycleServiceGrpc;
import com.acme.treasury.v1.ExpireCashPositionRequest;
import com.acme.treasury.v1.ExpireCashPositionResponse;
import com.acme.treasury.v1.GetCashPositionLifecycleRequest;
import com.acme.treasury.v1.GetCashPositionLifecycleResponse;
import com.acme.treasury.v1.ReinstateCashPositionRequest;
import com.acme.treasury.v1.ReinstateCashPositionResponse;
import com.acme.treasury.v1.RenewCashPositionRequest;
import com.acme.treasury.v1.RenewCashPositionResponse;
import com.acme.treasury.v1.ScheduleCashPositionExpiryRequest;
import com.acme.treasury.v1.ScheduleCashPositionExpiryResponse;
import com.acme.treasury.v1.SuspendCashPositionRequest;
import com.acme.treasury.v1.SuspendCashPositionResponse;
import com.acme.treasury.v1.TerminateCashPositionRequest;
import com.acme.treasury.v1.TerminateCashPositionResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Moves a cash position between the states of its lifecycle.
 */
@GrpcService
public class CashPositionLifecycleEndpoint extends CashPositionLifecycleServiceGrpc.CashPositionLifecycleServiceImplBase {
    protected final CashPositionLifecycleApplication application;

    public CashPositionLifecycleEndpoint(CashPositionLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates a cash position.
     */
    @Override
    public void activateCashPosition(ActivateCashPositionRequest request, StreamObserver<ActivateCashPositionResponse> observer) {
        RpcSupport.unary(observer, () -> application.activateCashPosition(request));
    }

    /**
     * Suspends a cash position.
     */
    @Override
    public void suspendCashPosition(SuspendCashPositionRequest request, StreamObserver<SuspendCashPositionResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendCashPosition(request));
    }

    /**
     * Reinstates a cash position.
     */
    @Override
    public void reinstateCashPosition(ReinstateCashPositionRequest request, StreamObserver<ReinstateCashPositionResponse> observer) {
        RpcSupport.unary(observer, () -> application.reinstateCashPosition(request));
    }

    /**
     * Terminates a cash position.
     */
    @Override
    public void terminateCashPosition(TerminateCashPositionRequest request, StreamObserver<TerminateCashPositionResponse> observer) {
        RpcSupport.unary(observer, () -> application.terminateCashPosition(request));
    }

    /**
     * Renews a cash position.
     */
    @Override
    public void renewCashPosition(RenewCashPositionRequest request, StreamObserver<RenewCashPositionResponse> observer) {
        RpcSupport.unary(observer, () -> application.renewCashPosition(request));
    }

    /**
     * Expires a cash position.
     */
    @Override
    public void expireCashPosition(ExpireCashPositionRequest request, StreamObserver<ExpireCashPositionResponse> observer) {
        RpcSupport.unary(observer, () -> application.expireCashPosition(request));
    }

    /**
     * Returns a cash position.
     */
    @Override
    public void getCashPositionLifecycle(GetCashPositionLifecycleRequest request, StreamObserver<GetCashPositionLifecycleResponse> observer) {
        RpcSupport.unary(observer, () -> application.getCashPositionLifecycle(request));
    }

    /**
     * Schedules a cash position.
     */
    @Override
    public void scheduleCashPositionExpiry(ScheduleCashPositionExpiryRequest request, StreamObserver<ScheduleCashPositionExpiryResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleCashPositionExpiry(request));
    }
}
