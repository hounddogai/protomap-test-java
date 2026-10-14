package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.ActivatePremiumRequest;
import com.acme.insurance.v1.ActivatePremiumResponse;
import com.acme.insurance.v1.ExpirePremiumRequest;
import com.acme.insurance.v1.ExpirePremiumResponse;
import com.acme.insurance.v1.GetPremiumLifecycleRequest;
import com.acme.insurance.v1.GetPremiumLifecycleResponse;
import com.acme.insurance.v1.PremiumLifecycleServiceGrpc;
import com.acme.insurance.v1.ReinstatePremiumRequest;
import com.acme.insurance.v1.ReinstatePremiumResponse;
import com.acme.insurance.v1.RenewPremiumRequest;
import com.acme.insurance.v1.RenewPremiumResponse;
import com.acme.insurance.v1.SchedulePremiumExpiryRequest;
import com.acme.insurance.v1.SchedulePremiumExpiryResponse;
import com.acme.insurance.v1.SuspendPremiumRequest;
import com.acme.insurance.v1.SuspendPremiumResponse;
import com.acme.insurance.v1.TerminatePremiumRequest;
import com.acme.insurance.v1.TerminatePremiumResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Moves a premium between the states of its lifecycle.
 */
@GrpcService
public class PremiumLifecycleEndpoint extends PremiumLifecycleServiceGrpc.PremiumLifecycleServiceImplBase {
    protected final PremiumLifecycleApplication application;

    public PremiumLifecycleEndpoint(PremiumLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates a premium.
     */
    @Override
    public void activatePremium(ActivatePremiumRequest request, StreamObserver<ActivatePremiumResponse> observer) {
        RpcSupport.unary(observer, () -> application.activatePremium(request));
    }

    /**
     * Suspends a premium.
     */
    @Override
    public void suspendPremium(SuspendPremiumRequest request, StreamObserver<SuspendPremiumResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendPremium(request));
    }

    /**
     * Reinstates a premium.
     */
    @Override
    public void reinstatePremium(ReinstatePremiumRequest request, StreamObserver<ReinstatePremiumResponse> observer) {
        RpcSupport.unary(observer, () -> application.reinstatePremium(request));
    }

    /**
     * Terminates a premium.
     */
    @Override
    public void terminatePremium(TerminatePremiumRequest request, StreamObserver<TerminatePremiumResponse> observer) {
        RpcSupport.unary(observer, () -> application.terminatePremium(request));
    }

    /**
     * Renews a premium.
     */
    @Override
    public void renewPremium(RenewPremiumRequest request, StreamObserver<RenewPremiumResponse> observer) {
        RpcSupport.unary(observer, () -> application.renewPremium(request));
    }

    /**
     * Expires a premium.
     */
    @Override
    public void expirePremium(ExpirePremiumRequest request, StreamObserver<ExpirePremiumResponse> observer) {
        RpcSupport.unary(observer, () -> application.expirePremium(request));
    }

    /**
     * Returns a premium.
     */
    @Override
    public void getPremiumLifecycle(GetPremiumLifecycleRequest request, StreamObserver<GetPremiumLifecycleResponse> observer) {
        RpcSupport.unary(observer, () -> application.getPremiumLifecycle(request));
    }

    /**
     * Schedules a premium.
     */
    @Override
    public void schedulePremiumExpiry(SchedulePremiumExpiryRequest request, StreamObserver<SchedulePremiumExpiryResponse> observer) {
        RpcSupport.unary(observer, () -> application.schedulePremiumExpiry(request));
    }
}
