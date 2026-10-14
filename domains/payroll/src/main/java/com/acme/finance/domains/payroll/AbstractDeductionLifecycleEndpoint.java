package com.acme.finance.domains.payroll;

import com.acme.finance.platform.RpcSupport;
import com.acme.payroll.v1.ActivateDeductionRequest;
import com.acme.payroll.v1.ActivateDeductionResponse;
import com.acme.payroll.v1.DeductionLifecycleServiceGrpc;
import com.acme.payroll.v1.GetDeductionLifecycleRequest;
import com.acme.payroll.v1.GetDeductionLifecycleResponse;
import com.acme.payroll.v1.ReinstateDeductionRequest;
import com.acme.payroll.v1.ReinstateDeductionResponse;
import com.acme.payroll.v1.RenewDeductionRequest;
import com.acme.payroll.v1.RenewDeductionResponse;
import com.acme.payroll.v1.ScheduleDeductionExpiryRequest;
import com.acme.payroll.v1.ScheduleDeductionExpiryResponse;
import com.acme.payroll.v1.SuspendDeductionRequest;
import com.acme.payroll.v1.SuspendDeductionResponse;
import com.acme.payroll.v1.TerminateDeductionRequest;
import com.acme.payroll.v1.TerminateDeductionResponse;
import io.grpc.stub.StreamObserver;

/**
 * Moves a deduction between the states of its lifecycle.
 */
public abstract class AbstractDeductionLifecycleEndpoint extends DeductionLifecycleServiceGrpc.DeductionLifecycleServiceImplBase {
    protected final DeductionLifecycleApplication application;

    protected AbstractDeductionLifecycleEndpoint(DeductionLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates a deduction.
     */
    @Override
    public void activateDeduction(ActivateDeductionRequest request, StreamObserver<ActivateDeductionResponse> observer) {
        RpcSupport.unary(observer, () -> application.activateDeduction(request));
    }

    /**
     * Suspends a deduction.
     */
    @Override
    public void suspendDeduction(SuspendDeductionRequest request, StreamObserver<SuspendDeductionResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendDeduction(request));
    }

    /**
     * Reinstates a deduction.
     */
    @Override
    public void reinstateDeduction(ReinstateDeductionRequest request, StreamObserver<ReinstateDeductionResponse> observer) {
        RpcSupport.unary(observer, () -> application.reinstateDeduction(request));
    }

    /**
     * Terminates a deduction.
     */
    @Override
    public void terminateDeduction(TerminateDeductionRequest request, StreamObserver<TerminateDeductionResponse> observer) {
        RpcSupport.unary(observer, () -> application.terminateDeduction(request));
    }

    /**
     * Renews a deduction.
     */
    @Override
    public void renewDeduction(RenewDeductionRequest request, StreamObserver<RenewDeductionResponse> observer) {
        RpcSupport.unary(observer, () -> application.renewDeduction(request));
    }

    /**
     * Returns a deduction.
     */
    @Override
    public void getDeductionLifecycle(GetDeductionLifecycleRequest request, StreamObserver<GetDeductionLifecycleResponse> observer) {
        RpcSupport.unary(observer, () -> application.getDeductionLifecycle(request));
    }

    /**
     * Schedules a deduction.
     */
    @Override
    public void scheduleDeductionExpiry(ScheduleDeductionExpiryRequest request, StreamObserver<ScheduleDeductionExpiryResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleDeductionExpiry(request));
    }
}
