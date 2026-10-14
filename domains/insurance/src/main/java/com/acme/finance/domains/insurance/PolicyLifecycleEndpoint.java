package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.ActivatePolicyRequest;
import com.acme.insurance.v1.ActivatePolicyResponse;
import com.acme.insurance.v1.ExpirePolicyRequest;
import com.acme.insurance.v1.ExpirePolicyResponse;
import com.acme.insurance.v1.GetPolicyLifecycleRequest;
import com.acme.insurance.v1.GetPolicyLifecycleResponse;
import com.acme.insurance.v1.PolicyLifecycleServiceGrpc;
import com.acme.insurance.v1.ReinstatePolicyRequest;
import com.acme.insurance.v1.ReinstatePolicyResponse;
import com.acme.insurance.v1.RenewPolicyRequest;
import com.acme.insurance.v1.RenewPolicyResponse;
import com.acme.insurance.v1.SchedulePolicyExpiryRequest;
import com.acme.insurance.v1.SchedulePolicyExpiryResponse;
import com.acme.insurance.v1.SuspendPolicyRequest;
import com.acme.insurance.v1.SuspendPolicyResponse;
import com.acme.insurance.v1.TerminatePolicyRequest;
import com.acme.insurance.v1.TerminatePolicyResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Moves a policy between the states of its lifecycle.
 */
@GrpcService
public class PolicyLifecycleEndpoint extends PolicyLifecycleServiceGrpc.PolicyLifecycleServiceImplBase {
    protected final PolicyLifecycleApplication application;

    public PolicyLifecycleEndpoint(PolicyLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates a policy.
     */
    @Override
    public void activatePolicy(ActivatePolicyRequest request, StreamObserver<ActivatePolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.activatePolicy(request));
    }

    /**
     * Suspends a policy.
     */
    @Override
    public void suspendPolicy(SuspendPolicyRequest request, StreamObserver<SuspendPolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendPolicy(request));
    }

    /**
     * Reinstates a policy.
     */
    @Override
    public void reinstatePolicy(ReinstatePolicyRequest request, StreamObserver<ReinstatePolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.reinstatePolicy(request));
    }

    /**
     * Terminates a policy.
     */
    @Override
    public void terminatePolicy(TerminatePolicyRequest request, StreamObserver<TerminatePolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.terminatePolicy(request));
    }

    /**
     * Renews a policy.
     */
    @Override
    public void renewPolicy(RenewPolicyRequest request, StreamObserver<RenewPolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.renewPolicy(request));
    }

    /**
     * Expires a policy.
     */
    @Override
    public void expirePolicy(ExpirePolicyRequest request, StreamObserver<ExpirePolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.expirePolicy(request));
    }

    /**
     * Returns a policy.
     */
    @Override
    public void getPolicyLifecycle(GetPolicyLifecycleRequest request, StreamObserver<GetPolicyLifecycleResponse> observer) {
        RpcSupport.unary(observer, () -> application.getPolicyLifecycle(request));
    }

    /**
     * Schedules a policy.
     */
    @Override
    public void schedulePolicyExpiry(SchedulePolicyExpiryRequest request, StreamObserver<SchedulePolicyExpiryResponse> observer) {
        RpcSupport.unary(observer, () -> application.schedulePolicyExpiry(request));
    }
}
