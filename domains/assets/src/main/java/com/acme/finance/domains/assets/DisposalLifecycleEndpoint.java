package com.acme.finance.domains.assets;

import com.acme.assets.v1.ActivateDisposalRequest;
import com.acme.assets.v1.ActivateDisposalResponse;
import com.acme.assets.v1.DisposalLifecycleServiceGrpc;
import com.acme.assets.v1.ExpireDisposalRequest;
import com.acme.assets.v1.ExpireDisposalResponse;
import com.acme.assets.v1.GetDisposalLifecycleRequest;
import com.acme.assets.v1.GetDisposalLifecycleResponse;
import com.acme.assets.v1.ReinstateDisposalRequest;
import com.acme.assets.v1.ReinstateDisposalResponse;
import com.acme.assets.v1.RenewDisposalRequest;
import com.acme.assets.v1.RenewDisposalResponse;
import com.acme.assets.v1.ScheduleDisposalExpiryRequest;
import com.acme.assets.v1.ScheduleDisposalExpiryResponse;
import com.acme.assets.v1.SuspendDisposalRequest;
import com.acme.assets.v1.SuspendDisposalResponse;
import com.acme.assets.v1.TerminateDisposalRequest;
import com.acme.assets.v1.TerminateDisposalResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Moves a disposal between the states of its lifecycle.
 */
@GrpcService
public class DisposalLifecycleEndpoint extends DisposalLifecycleServiceGrpc.DisposalLifecycleServiceImplBase {
    protected final DisposalLifecycleApplication application;

    public DisposalLifecycleEndpoint(DisposalLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates a disposal.
     */
    @Override
    public void activateDisposal(ActivateDisposalRequest request, StreamObserver<ActivateDisposalResponse> observer) {
        RpcSupport.unary(observer, () -> application.activateDisposal(request));
    }

    /**
     * Suspends a disposal.
     */
    @Override
    public void suspendDisposal(SuspendDisposalRequest request, StreamObserver<SuspendDisposalResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendDisposal(request));
    }

    /**
     * Reinstates a disposal.
     */
    @Override
    public void reinstateDisposal(ReinstateDisposalRequest request, StreamObserver<ReinstateDisposalResponse> observer) {
        RpcSupport.unary(observer, () -> application.reinstateDisposal(request));
    }

    /**
     * Terminates a disposal.
     */
    @Override
    public void terminateDisposal(TerminateDisposalRequest request, StreamObserver<TerminateDisposalResponse> observer) {
        RpcSupport.unary(observer, () -> application.terminateDisposal(request));
    }

    /**
     * Renews a disposal.
     */
    @Override
    public void renewDisposal(RenewDisposalRequest request, StreamObserver<RenewDisposalResponse> observer) {
        RpcSupport.unary(observer, () -> application.renewDisposal(request));
    }

    /**
     * Expires a disposal.
     */
    @Override
    public void expireDisposal(ExpireDisposalRequest request, StreamObserver<ExpireDisposalResponse> observer) {
        RpcSupport.unary(observer, () -> application.expireDisposal(request));
    }

    /**
     * Returns a disposal.
     */
    @Override
    public void getDisposalLifecycle(GetDisposalLifecycleRequest request, StreamObserver<GetDisposalLifecycleResponse> observer) {
        RpcSupport.unary(observer, () -> application.getDisposalLifecycle(request));
    }

    /**
     * Schedules a disposal.
     */
    @Override
    public void scheduleDisposalExpiry(ScheduleDisposalExpiryRequest request, StreamObserver<ScheduleDisposalExpiryResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleDisposalExpiry(request));
    }
}
