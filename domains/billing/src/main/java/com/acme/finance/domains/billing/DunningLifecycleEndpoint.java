package com.acme.finance.domains.billing;

import com.acme.billing.v1.ActivateDunningRequest;
import com.acme.billing.v1.ActivateDunningResponse;
import com.acme.billing.v1.DunningLifecycleServiceGrpc;
import com.acme.billing.v1.ExpireDunningRequest;
import com.acme.billing.v1.ExpireDunningResponse;
import com.acme.billing.v1.GetDunningLifecycleRequest;
import com.acme.billing.v1.GetDunningLifecycleResponse;
import com.acme.billing.v1.ReinstateDunningRequest;
import com.acme.billing.v1.ReinstateDunningResponse;
import com.acme.billing.v1.RenewDunningRequest;
import com.acme.billing.v1.RenewDunningResponse;
import com.acme.billing.v1.ScheduleDunningExpiryRequest;
import com.acme.billing.v1.ScheduleDunningExpiryResponse;
import com.acme.billing.v1.SuspendDunningRequest;
import com.acme.billing.v1.SuspendDunningResponse;
import com.acme.billing.v1.TerminateDunningRequest;
import com.acme.billing.v1.TerminateDunningResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Moves a dunning between the states of its lifecycle.
 */
@GrpcService
public class DunningLifecycleEndpoint extends DunningLifecycleServiceGrpc.DunningLifecycleServiceImplBase {
    protected final DunningLifecycleApplication application;

    public DunningLifecycleEndpoint(DunningLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates a dunning.
     */
    @Override
    public void activateDunning(ActivateDunningRequest request, StreamObserver<ActivateDunningResponse> observer) {
        RpcSupport.unary(observer, () -> application.activateDunning(request));
    }

    /**
     * Suspends a dunning.
     */
    @Override
    public void suspendDunning(SuspendDunningRequest request, StreamObserver<SuspendDunningResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendDunning(request));
    }

    /**
     * Reinstates a dunning.
     */
    @Override
    public void reinstateDunning(ReinstateDunningRequest request, StreamObserver<ReinstateDunningResponse> observer) {
        RpcSupport.unary(observer, () -> application.reinstateDunning(request));
    }

    /**
     * Terminates a dunning.
     */
    @Override
    public void terminateDunning(TerminateDunningRequest request, StreamObserver<TerminateDunningResponse> observer) {
        RpcSupport.unary(observer, () -> application.terminateDunning(request));
    }

    /**
     * Renews a dunning.
     */
    @Override
    public void renewDunning(RenewDunningRequest request, StreamObserver<RenewDunningResponse> observer) {
        RpcSupport.unary(observer, () -> application.renewDunning(request));
    }

    /**
     * Expires a dunning.
     */
    @Override
    public void expireDunning(ExpireDunningRequest request, StreamObserver<ExpireDunningResponse> observer) {
        RpcSupport.unary(observer, () -> application.expireDunning(request));
    }

    /**
     * Returns a dunning.
     */
    @Override
    public void getDunningLifecycle(GetDunningLifecycleRequest request, StreamObserver<GetDunningLifecycleResponse> observer) {
        RpcSupport.unary(observer, () -> application.getDunningLifecycle(request));
    }

    /**
     * Schedules a dunning.
     */
    @Override
    public void scheduleDunningExpiry(ScheduleDunningExpiryRequest request, StreamObserver<ScheduleDunningExpiryResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleDunningExpiry(request));
    }
}
