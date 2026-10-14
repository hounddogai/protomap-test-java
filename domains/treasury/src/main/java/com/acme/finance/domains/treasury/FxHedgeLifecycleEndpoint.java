package com.acme.finance.domains.treasury;

import com.acme.finance.platform.RpcSupport;
import com.acme.treasury.v1.ActivateFxHedgeRequest;
import com.acme.treasury.v1.ActivateFxHedgeResponse;
import com.acme.treasury.v1.ExpireFxHedgeRequest;
import com.acme.treasury.v1.ExpireFxHedgeResponse;
import com.acme.treasury.v1.FxHedgeLifecycleServiceGrpc;
import com.acme.treasury.v1.GetFxHedgeLifecycleRequest;
import com.acme.treasury.v1.GetFxHedgeLifecycleResponse;
import com.acme.treasury.v1.ReinstateFxHedgeRequest;
import com.acme.treasury.v1.ReinstateFxHedgeResponse;
import com.acme.treasury.v1.RenewFxHedgeRequest;
import com.acme.treasury.v1.RenewFxHedgeResponse;
import com.acme.treasury.v1.SuspendFxHedgeRequest;
import com.acme.treasury.v1.SuspendFxHedgeResponse;
import com.acme.treasury.v1.TerminateFxHedgeRequest;
import com.acme.treasury.v1.TerminateFxHedgeResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Moves a fx hedge between the states of its lifecycle.
 */
@GrpcService
public class FxHedgeLifecycleEndpoint extends FxHedgeLifecycleServiceGrpc.FxHedgeLifecycleServiceImplBase {
    protected final FxHedgeLifecycleApplication application;

    public FxHedgeLifecycleEndpoint(FxHedgeLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates a fx hedge.
     */
    @Override
    public void activateFxHedge(ActivateFxHedgeRequest request, StreamObserver<ActivateFxHedgeResponse> observer) {
        RpcSupport.unary(observer, () -> application.activateFxHedge(request));
    }

    /**
     * Suspends a fx hedge.
     */
    @Override
    public void suspendFxHedge(SuspendFxHedgeRequest request, StreamObserver<SuspendFxHedgeResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendFxHedge(request));
    }

    /**
     * Reinstates a fx hedge.
     */
    @Override
    public void reinstateFxHedge(ReinstateFxHedgeRequest request, StreamObserver<ReinstateFxHedgeResponse> observer) {
        RpcSupport.unary(observer, () -> application.reinstateFxHedge(request));
    }

    /**
     * Terminates a fx hedge.
     */
    @Override
    public void terminateFxHedge(TerminateFxHedgeRequest request, StreamObserver<TerminateFxHedgeResponse> observer) {
        RpcSupport.unary(observer, () -> application.terminateFxHedge(request));
    }

    /**
     * Renews a fx hedge.
     */
    @Override
    public void renewFxHedge(RenewFxHedgeRequest request, StreamObserver<RenewFxHedgeResponse> observer) {
        RpcSupport.unary(observer, () -> application.renewFxHedge(request));
    }

    /**
     * Expires a fx hedge.
     */
    @Override
    public void expireFxHedge(ExpireFxHedgeRequest request, StreamObserver<ExpireFxHedgeResponse> observer) {
        RpcSupport.unary(observer, () -> application.expireFxHedge(request));
    }

    /**
     * Returns a fx hedge.
     */
    @Override
    public void getFxHedgeLifecycle(GetFxHedgeLifecycleRequest request, StreamObserver<GetFxHedgeLifecycleResponse> observer) {
        RpcSupport.unary(observer, () -> application.getFxHedgeLifecycle(request));
    }
}
