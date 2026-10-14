package com.acme.finance.domains.compliance;

import com.acme.compliance.v1.ActivateSanctionRequest;
import com.acme.compliance.v1.ActivateSanctionResponse;
import com.acme.compliance.v1.ExpireSanctionRequest;
import com.acme.compliance.v1.ExpireSanctionResponse;
import com.acme.compliance.v1.GetSanctionLifecycleRequest;
import com.acme.compliance.v1.GetSanctionLifecycleResponse;
import com.acme.compliance.v1.ReinstateSanctionRequest;
import com.acme.compliance.v1.ReinstateSanctionResponse;
import com.acme.compliance.v1.RenewSanctionRequest;
import com.acme.compliance.v1.RenewSanctionResponse;
import com.acme.compliance.v1.SanctionLifecycleServiceGrpc;
import com.acme.compliance.v1.ScheduleSanctionExpiryRequest;
import com.acme.compliance.v1.ScheduleSanctionExpiryResponse;
import com.acme.compliance.v1.SuspendSanctionRequest;
import com.acme.compliance.v1.SuspendSanctionResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Moves a sanction between the states of its lifecycle.
 */
public abstract class AbstractSanctionLifecycleEndpoint extends SanctionLifecycleServiceGrpc.SanctionLifecycleServiceImplBase {
    protected final SanctionLifecycleApplication application;

    protected AbstractSanctionLifecycleEndpoint(SanctionLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates a sanction.
     */
    @Override
    public void activateSanction(ActivateSanctionRequest request, StreamObserver<ActivateSanctionResponse> observer) {
        RpcSupport.unary(observer, () -> application.activateSanction(request));
    }

    /**
     * Suspends a sanction.
     */
    @Override
    public void suspendSanction(SuspendSanctionRequest request, StreamObserver<SuspendSanctionResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendSanction(request));
    }

    /**
     * Reinstates a sanction.
     */
    @Override
    public void reinstateSanction(ReinstateSanctionRequest request, StreamObserver<ReinstateSanctionResponse> observer) {
        RpcSupport.unary(observer, () -> application.reinstateSanction(request));
    }

    /**
     * Renews a sanction.
     */
    @Override
    public void renewSanction(RenewSanctionRequest request, StreamObserver<RenewSanctionResponse> observer) {
        RpcSupport.unary(observer, () -> application.renewSanction(request));
    }

    /**
     * Expires a sanction.
     */
    @Override
    public void expireSanction(ExpireSanctionRequest request, StreamObserver<ExpireSanctionResponse> observer) {
        RpcSupport.unary(observer, () -> application.expireSanction(request));
    }

    /**
     * Returns a sanction.
     */
    @Override
    public void getSanctionLifecycle(GetSanctionLifecycleRequest request, StreamObserver<GetSanctionLifecycleResponse> observer) {
        RpcSupport.unary(observer, () -> application.getSanctionLifecycle(request));
    }

    /**
     * Schedules a sanction.
     */
    @Override
    public void scheduleSanctionExpiry(ScheduleSanctionExpiryRequest request, StreamObserver<ScheduleSanctionExpiryResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleSanctionExpiry(request));
    }
}
