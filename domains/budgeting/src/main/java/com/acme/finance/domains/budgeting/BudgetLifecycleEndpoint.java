package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.ActivateBudgetRequest;
import com.acme.budgeting.v1.ActivateBudgetResponse;
import com.acme.budgeting.v1.BudgetLifecycleServiceGrpc;
import com.acme.budgeting.v1.ExpireBudgetRequest;
import com.acme.budgeting.v1.ExpireBudgetResponse;
import com.acme.budgeting.v1.GetBudgetLifecycleRequest;
import com.acme.budgeting.v1.GetBudgetLifecycleResponse;
import com.acme.budgeting.v1.ReinstateBudgetRequest;
import com.acme.budgeting.v1.ReinstateBudgetResponse;
import com.acme.budgeting.v1.RenewBudgetRequest;
import com.acme.budgeting.v1.RenewBudgetResponse;
import com.acme.budgeting.v1.ScheduleBudgetExpiryRequest;
import com.acme.budgeting.v1.ScheduleBudgetExpiryResponse;
import com.acme.budgeting.v1.SuspendBudgetRequest;
import com.acme.budgeting.v1.SuspendBudgetResponse;
import com.acme.budgeting.v1.TerminateBudgetRequest;
import com.acme.budgeting.v1.TerminateBudgetResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Moves a budget between the states of its lifecycle.
 */
@GrpcService
public class BudgetLifecycleEndpoint extends BudgetLifecycleServiceGrpc.BudgetLifecycleServiceImplBase {
    protected final BudgetLifecycleApplication application;

    public BudgetLifecycleEndpoint(BudgetLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates a budget.
     */
    @Override
    public void activateBudget(ActivateBudgetRequest request, StreamObserver<ActivateBudgetResponse> observer) {
        RpcSupport.unary(observer, () -> application.activateBudget(request));
    }

    /**
     * Suspends a budget.
     */
    @Override
    public void suspendBudget(SuspendBudgetRequest request, StreamObserver<SuspendBudgetResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendBudget(request));
    }

    /**
     * Reinstates a budget.
     */
    @Override
    public void reinstateBudget(ReinstateBudgetRequest request, StreamObserver<ReinstateBudgetResponse> observer) {
        RpcSupport.unary(observer, () -> application.reinstateBudget(request));
    }

    /**
     * Terminates a budget.
     */
    @Override
    public void terminateBudget(TerminateBudgetRequest request, StreamObserver<TerminateBudgetResponse> observer) {
        RpcSupport.unary(observer, () -> application.terminateBudget(request));
    }

    /**
     * Renews a budget.
     */
    @Override
    public void renewBudget(RenewBudgetRequest request, StreamObserver<RenewBudgetResponse> observer) {
        RpcSupport.unary(observer, () -> application.renewBudget(request));
    }

    /**
     * Expires a budget.
     */
    @Override
    public void expireBudget(ExpireBudgetRequest request, StreamObserver<ExpireBudgetResponse> observer) {
        RpcSupport.unary(observer, () -> application.expireBudget(request));
    }

    /**
     * Returns a budget.
     */
    @Override
    public void getBudgetLifecycle(GetBudgetLifecycleRequest request, StreamObserver<GetBudgetLifecycleResponse> observer) {
        RpcSupport.unary(observer, () -> application.getBudgetLifecycle(request));
    }

    /**
     * Schedules a budget.
     */
    @Override
    public void scheduleBudgetExpiry(ScheduleBudgetExpiryRequest request, StreamObserver<ScheduleBudgetExpiryResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleBudgetExpiry(request));
    }
}
