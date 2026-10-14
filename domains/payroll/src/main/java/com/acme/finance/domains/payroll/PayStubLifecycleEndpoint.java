package com.acme.finance.domains.payroll;

import com.acme.finance.platform.RpcSupport;
import com.acme.payroll.v1.ActivatePayStubRequest;
import com.acme.payroll.v1.ActivatePayStubResponse;
import com.acme.payroll.v1.ExpirePayStubRequest;
import com.acme.payroll.v1.ExpirePayStubResponse;
import com.acme.payroll.v1.GetPayStubLifecycleRequest;
import com.acme.payroll.v1.GetPayStubLifecycleResponse;
import com.acme.payroll.v1.PayStubLifecycleServiceGrpc;
import com.acme.payroll.v1.ReinstatePayStubRequest;
import com.acme.payroll.v1.ReinstatePayStubResponse;
import com.acme.payroll.v1.RenewPayStubRequest;
import com.acme.payroll.v1.RenewPayStubResponse;
import com.acme.payroll.v1.SchedulePayStubExpiryRequest;
import com.acme.payroll.v1.SchedulePayStubExpiryResponse;
import com.acme.payroll.v1.SuspendPayStubRequest;
import com.acme.payroll.v1.SuspendPayStubResponse;
import com.acme.payroll.v1.TerminatePayStubRequest;
import com.acme.payroll.v1.TerminatePayStubResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Moves a pay stub between the states of its lifecycle.
 */
@GrpcService
public class PayStubLifecycleEndpoint extends PayStubLifecycleServiceGrpc.PayStubLifecycleServiceImplBase {
    protected final PayStubLifecycleApplication application;

    public PayStubLifecycleEndpoint(PayStubLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates a pay stub.
     */
    @Override
    public void activatePayStub(ActivatePayStubRequest request, StreamObserver<ActivatePayStubResponse> observer) {
        RpcSupport.unary(observer, () -> application.activatePayStub(request));
    }

    /**
     * Suspends a pay stub.
     */
    @Override
    public void suspendPayStub(SuspendPayStubRequest request, StreamObserver<SuspendPayStubResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendPayStub(request));
    }

    /**
     * Reinstates a pay stub.
     */
    @Override
    public void reinstatePayStub(ReinstatePayStubRequest request, StreamObserver<ReinstatePayStubResponse> observer) {
        RpcSupport.unary(observer, () -> application.reinstatePayStub(request));
    }

    /**
     * Terminates a pay stub.
     */
    @Override
    public void terminatePayStub(TerminatePayStubRequest request, StreamObserver<TerminatePayStubResponse> observer) {
        RpcSupport.unary(observer, () -> application.terminatePayStub(request));
    }

    /**
     * Renews a pay stub.
     */
    @Override
    public void renewPayStub(RenewPayStubRequest request, StreamObserver<RenewPayStubResponse> observer) {
        RpcSupport.unary(observer, () -> application.renewPayStub(request));
    }

    /**
     * Expires a pay stub.
     */
    @Override
    public void expirePayStub(ExpirePayStubRequest request, StreamObserver<ExpirePayStubResponse> observer) {
        RpcSupport.unary(observer, () -> application.expirePayStub(request));
    }

    /**
     * Returns a pay stub.
     */
    @Override
    public void getPayStubLifecycle(GetPayStubLifecycleRequest request, StreamObserver<GetPayStubLifecycleResponse> observer) {
        RpcSupport.unary(observer, () -> application.getPayStubLifecycle(request));
    }

    /**
     * Schedules a pay stub.
     */
    @Override
    public void schedulePayStubExpiry(SchedulePayStubExpiryRequest request, StreamObserver<SchedulePayStubExpiryResponse> observer) {
        RpcSupport.unary(observer, () -> application.schedulePayStubExpiry(request));
    }
}
