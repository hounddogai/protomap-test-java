package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.ActivateBudgetLineRequest;
import com.acme.budgeting.v1.ActivateBudgetLineResponse;
import com.acme.budgeting.v1.BudgetLineLifecycleServiceGrpc;
import com.acme.budgeting.v1.ExpireBudgetLineRequest;
import com.acme.budgeting.v1.ExpireBudgetLineResponse;
import com.acme.budgeting.v1.GetBudgetLineLifecycleRequest;
import com.acme.budgeting.v1.GetBudgetLineLifecycleResponse;
import com.acme.budgeting.v1.ReinstateBudgetLineRequest;
import com.acme.budgeting.v1.ReinstateBudgetLineResponse;
import com.acme.budgeting.v1.ScheduleBudgetLineExpiryRequest;
import com.acme.budgeting.v1.ScheduleBudgetLineExpiryResponse;
import com.acme.budgeting.v1.SuspendBudgetLineRequest;
import com.acme.budgeting.v1.SuspendBudgetLineResponse;
import com.acme.budgeting.v1.TerminateBudgetLineRequest;
import com.acme.budgeting.v1.TerminateBudgetLineResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Moves a budget line between the states of its lifecycle.
 */
@GrpcService
public class BudgetLineLifecycleEndpoint extends BudgetLineLifecycleServiceGrpc.BudgetLineLifecycleServiceImplBase {
    protected final BudgetLineLifecycleApplication application;

    public BudgetLineLifecycleEndpoint(BudgetLineLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates a budget line.
     */
    @Override
    public void activateBudgetLine(ActivateBudgetLineRequest request, StreamObserver<ActivateBudgetLineResponse> observer) {
        RpcSupport.unary(observer, () -> application.activateBudgetLine(request));
    }

    /**
     * Suspends a budget line.
     */
    @Override
    public void suspendBudgetLine(SuspendBudgetLineRequest request, StreamObserver<SuspendBudgetLineResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendBudgetLine(request));
    }

    /**
     * Reinstates a budget line.
     */
    @Override
    public void reinstateBudgetLine(ReinstateBudgetLineRequest request, StreamObserver<ReinstateBudgetLineResponse> observer) {
        RpcSupport.unary(observer, () -> application.reinstateBudgetLine(request));
    }

    /**
     * Terminates a budget line.
     */
    @Override
    public void terminateBudgetLine(TerminateBudgetLineRequest request, StreamObserver<TerminateBudgetLineResponse> observer) {
        RpcSupport.unary(observer, () -> application.terminateBudgetLine(request));
    }

    /**
     * Expires a budget line.
     */
    @Override
    public void expireBudgetLine(ExpireBudgetLineRequest request, StreamObserver<ExpireBudgetLineResponse> observer) {
        RpcSupport.unary(observer, () -> application.expireBudgetLine(request));
    }

    /**
     * Returns a budget line.
     */
    @Override
    public void getBudgetLineLifecycle(GetBudgetLineLifecycleRequest request, StreamObserver<GetBudgetLineLifecycleResponse> observer) {
        RpcSupport.unary(observer, () -> application.getBudgetLineLifecycle(request));
    }

    /**
     * Schedules a budget line.
     */
    @Override
    public void scheduleBudgetLineExpiry(ScheduleBudgetLineExpiryRequest request, StreamObserver<ScheduleBudgetLineExpiryResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleBudgetLineExpiry(request));
    }
}
