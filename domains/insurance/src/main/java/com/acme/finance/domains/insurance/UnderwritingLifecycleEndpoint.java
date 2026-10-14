package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.ActivateUnderwritingRequest;
import com.acme.insurance.v1.ActivateUnderwritingResponse;
import com.acme.insurance.v1.ExpireUnderwritingRequest;
import com.acme.insurance.v1.ExpireUnderwritingResponse;
import com.acme.insurance.v1.GetUnderwritingLifecycleRequest;
import com.acme.insurance.v1.GetUnderwritingLifecycleResponse;
import com.acme.insurance.v1.ReinstateUnderwritingRequest;
import com.acme.insurance.v1.ReinstateUnderwritingResponse;
import com.acme.insurance.v1.RenewUnderwritingRequest;
import com.acme.insurance.v1.RenewUnderwritingResponse;
import com.acme.insurance.v1.ScheduleUnderwritingExpiryRequest;
import com.acme.insurance.v1.ScheduleUnderwritingExpiryResponse;
import com.acme.insurance.v1.SuspendUnderwritingRequest;
import com.acme.insurance.v1.SuspendUnderwritingResponse;
import com.acme.insurance.v1.TerminateUnderwritingRequest;
import com.acme.insurance.v1.TerminateUnderwritingResponse;
import com.acme.insurance.v1.UnderwritingLifecycleServiceGrpc;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Moves an underwriting between the states of its lifecycle.
 */
@GrpcService
public class UnderwritingLifecycleEndpoint extends UnderwritingLifecycleServiceGrpc.UnderwritingLifecycleServiceImplBase {
    protected final UnderwritingLifecycleApplication application;

    public UnderwritingLifecycleEndpoint(UnderwritingLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates an underwriting.
     */
    @Override
    public void activateUnderwriting(ActivateUnderwritingRequest request, StreamObserver<ActivateUnderwritingResponse> observer) {
        RpcSupport.unary(observer, () -> application.activateUnderwriting(request));
    }

    /**
     * Suspends an underwriting.
     */
    @Override
    public void suspendUnderwriting(SuspendUnderwritingRequest request, StreamObserver<SuspendUnderwritingResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendUnderwriting(request));
    }

    /**
     * Reinstates an underwriting.
     */
    @Override
    public void reinstateUnderwriting(ReinstateUnderwritingRequest request, StreamObserver<ReinstateUnderwritingResponse> observer) {
        RpcSupport.unary(observer, () -> application.reinstateUnderwriting(request));
    }

    /**
     * Terminates an underwriting.
     */
    @Override
    public void terminateUnderwriting(TerminateUnderwritingRequest request, StreamObserver<TerminateUnderwritingResponse> observer) {
        RpcSupport.unary(observer, () -> application.terminateUnderwriting(request));
    }

    /**
     * Renews an underwriting.
     */
    @Override
    public void renewUnderwriting(RenewUnderwritingRequest request, StreamObserver<RenewUnderwritingResponse> observer) {
        RpcSupport.unary(observer, () -> application.renewUnderwriting(request));
    }

    /**
     * Expires an underwriting.
     */
    @Override
    public void expireUnderwriting(ExpireUnderwritingRequest request, StreamObserver<ExpireUnderwritingResponse> observer) {
        RpcSupport.unary(observer, () -> application.expireUnderwriting(request));
    }

    /**
     * Returns an underwriting.
     */
    @Override
    public void getUnderwritingLifecycle(GetUnderwritingLifecycleRequest request, StreamObserver<GetUnderwritingLifecycleResponse> observer) {
        RpcSupport.unary(observer, () -> application.getUnderwritingLifecycle(request));
    }

    /**
     * Schedules an underwriting.
     */
    @Override
    public void scheduleUnderwritingExpiry(ScheduleUnderwritingExpiryRequest request, StreamObserver<ScheduleUnderwritingExpiryResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleUnderwritingExpiry(request));
    }
}
