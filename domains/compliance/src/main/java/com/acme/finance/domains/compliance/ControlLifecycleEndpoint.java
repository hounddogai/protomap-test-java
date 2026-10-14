package com.acme.finance.domains.compliance;

import com.acme.compliance.v1.ActivateControlRequest;
import com.acme.compliance.v1.ActivateControlResponse;
import com.acme.compliance.v1.ControlLifecycleServiceGrpc;
import com.acme.compliance.v1.ExpireControlRequest;
import com.acme.compliance.v1.ExpireControlResponse;
import com.acme.compliance.v1.GetControlLifecycleRequest;
import com.acme.compliance.v1.GetControlLifecycleResponse;
import com.acme.compliance.v1.ReinstateControlRequest;
import com.acme.compliance.v1.ReinstateControlResponse;
import com.acme.compliance.v1.RenewControlRequest;
import com.acme.compliance.v1.RenewControlResponse;
import com.acme.compliance.v1.ScheduleControlExpiryRequest;
import com.acme.compliance.v1.ScheduleControlExpiryResponse;
import com.acme.compliance.v1.SuspendControlRequest;
import com.acme.compliance.v1.SuspendControlResponse;
import com.acme.compliance.v1.TerminateControlRequest;
import com.acme.compliance.v1.TerminateControlResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Moves a control between the states of its lifecycle.
 */
@GrpcService
public class ControlLifecycleEndpoint extends ControlLifecycleServiceGrpc.ControlLifecycleServiceImplBase {
    protected final ControlLifecycleApplication application;

    public ControlLifecycleEndpoint(ControlLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates a control.
     */
    @Override
    public void activateControl(ActivateControlRequest request, StreamObserver<ActivateControlResponse> observer) {
        RpcSupport.unary(observer, () -> application.activateControl(request));
    }

    /**
     * Suspends a control.
     */
    @Override
    public void suspendControl(SuspendControlRequest request, StreamObserver<SuspendControlResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendControl(request));
    }

    /**
     * Reinstates a control.
     */
    @Override
    public void reinstateControl(ReinstateControlRequest request, StreamObserver<ReinstateControlResponse> observer) {
        RpcSupport.unary(observer, () -> application.reinstateControl(request));
    }

    /**
     * Terminates a control.
     */
    @Override
    public void terminateControl(TerminateControlRequest request, StreamObserver<TerminateControlResponse> observer) {
        RpcSupport.unary(observer, () -> application.terminateControl(request));
    }

    /**
     * Renews a control.
     */
    @Override
    public void renewControl(RenewControlRequest request, StreamObserver<RenewControlResponse> observer) {
        RpcSupport.unary(observer, () -> application.renewControl(request));
    }

    /**
     * Expires a control.
     */
    @Override
    public void expireControl(ExpireControlRequest request, StreamObserver<ExpireControlResponse> observer) {
        RpcSupport.unary(observer, () -> application.expireControl(request));
    }

    /**
     * Returns a control.
     */
    @Override
    public void getControlLifecycle(GetControlLifecycleRequest request, StreamObserver<GetControlLifecycleResponse> observer) {
        RpcSupport.unary(observer, () -> application.getControlLifecycle(request));
    }

    /**
     * Schedules a control.
     */
    @Override
    public void scheduleControlExpiry(ScheduleControlExpiryRequest request, StreamObserver<ScheduleControlExpiryResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleControlExpiry(request));
    }
}
