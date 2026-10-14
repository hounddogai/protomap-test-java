package com.acme.finance.domains.expenses;

import com.acme.expenses.v1.ActivateExpensePolicyRequest;
import com.acme.expenses.v1.ActivateExpensePolicyResponse;
import com.acme.expenses.v1.ExpensePolicyLifecycleServiceGrpc;
import com.acme.expenses.v1.ExpireExpensePolicyRequest;
import com.acme.expenses.v1.ExpireExpensePolicyResponse;
import com.acme.expenses.v1.GetExpensePolicyLifecycleRequest;
import com.acme.expenses.v1.GetExpensePolicyLifecycleResponse;
import com.acme.expenses.v1.ReinstateExpensePolicyRequest;
import com.acme.expenses.v1.ReinstateExpensePolicyResponse;
import com.acme.expenses.v1.RenewExpensePolicyRequest;
import com.acme.expenses.v1.RenewExpensePolicyResponse;
import com.acme.expenses.v1.ScheduleExpensePolicyExpiryRequest;
import com.acme.expenses.v1.ScheduleExpensePolicyExpiryResponse;
import com.acme.expenses.v1.SuspendExpensePolicyRequest;
import com.acme.expenses.v1.SuspendExpensePolicyResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Moves an expense policy between the states of its lifecycle.
 */
public abstract class AbstractExpensePolicyLifecycleEndpoint extends ExpensePolicyLifecycleServiceGrpc.ExpensePolicyLifecycleServiceImplBase {
    protected final ExpensePolicyLifecycleApplication application;

    protected AbstractExpensePolicyLifecycleEndpoint(ExpensePolicyLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates an expense policy.
     */
    @Override
    public void activateExpensePolicy(ActivateExpensePolicyRequest request, StreamObserver<ActivateExpensePolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.activateExpensePolicy(request));
    }

    /**
     * Suspends an expense policy.
     */
    @Override
    public void suspendExpensePolicy(SuspendExpensePolicyRequest request, StreamObserver<SuspendExpensePolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendExpensePolicy(request));
    }

    /**
     * Reinstates an expense policy.
     */
    @Override
    public void reinstateExpensePolicy(ReinstateExpensePolicyRequest request, StreamObserver<ReinstateExpensePolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.reinstateExpensePolicy(request));
    }

    /**
     * Renews an expense policy.
     */
    @Override
    public void renewExpensePolicy(RenewExpensePolicyRequest request, StreamObserver<RenewExpensePolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.renewExpensePolicy(request));
    }

    /**
     * Expires an expense policy.
     */
    @Override
    public void expireExpensePolicy(ExpireExpensePolicyRequest request, StreamObserver<ExpireExpensePolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.expireExpensePolicy(request));
    }

    /**
     * Returns an expense policy.
     */
    @Override
    public void getExpensePolicyLifecycle(GetExpensePolicyLifecycleRequest request, StreamObserver<GetExpensePolicyLifecycleResponse> observer) {
        RpcSupport.unary(observer, () -> application.getExpensePolicyLifecycle(request));
    }

    /**
     * Schedules an expense policy.
     */
    @Override
    public void scheduleExpensePolicyExpiry(ScheduleExpensePolicyExpiryRequest request, StreamObserver<ScheduleExpensePolicyExpiryResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleExpensePolicyExpiry(request));
    }
}
