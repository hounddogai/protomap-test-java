package com.acme.finance.domains.expenses;

import com.acme.expenses.v1.ActivateMileageClaimRequest;
import com.acme.expenses.v1.ActivateMileageClaimResponse;
import com.acme.expenses.v1.ExpireMileageClaimRequest;
import com.acme.expenses.v1.ExpireMileageClaimResponse;
import com.acme.expenses.v1.GetMileageClaimLifecycleRequest;
import com.acme.expenses.v1.GetMileageClaimLifecycleResponse;
import com.acme.expenses.v1.MileageClaimLifecycleServiceGrpc;
import com.acme.expenses.v1.ReinstateMileageClaimRequest;
import com.acme.expenses.v1.ReinstateMileageClaimResponse;
import com.acme.expenses.v1.RenewMileageClaimRequest;
import com.acme.expenses.v1.RenewMileageClaimResponse;
import com.acme.expenses.v1.ScheduleMileageClaimExpiryRequest;
import com.acme.expenses.v1.ScheduleMileageClaimExpiryResponse;
import com.acme.expenses.v1.SuspendMileageClaimRequest;
import com.acme.expenses.v1.SuspendMileageClaimResponse;
import com.acme.expenses.v1.TerminateMileageClaimRequest;
import com.acme.expenses.v1.TerminateMileageClaimResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Moves a mileage claim between the states of its lifecycle.
 */
@GrpcService
public class MileageClaimLifecycleEndpoint extends MileageClaimLifecycleServiceGrpc.MileageClaimLifecycleServiceImplBase {
    protected final MileageClaimLifecycleApplication application;

    public MileageClaimLifecycleEndpoint(MileageClaimLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates a mileage claim.
     */
    @Override
    public void activateMileageClaim(ActivateMileageClaimRequest request, StreamObserver<ActivateMileageClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.activateMileageClaim(request));
    }

    /**
     * Suspends a mileage claim.
     */
    @Override
    public void suspendMileageClaim(SuspendMileageClaimRequest request, StreamObserver<SuspendMileageClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendMileageClaim(request));
    }

    /**
     * Reinstates a mileage claim.
     */
    @Override
    public void reinstateMileageClaim(ReinstateMileageClaimRequest request, StreamObserver<ReinstateMileageClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.reinstateMileageClaim(request));
    }

    /**
     * Terminates a mileage claim.
     */
    @Override
    public void terminateMileageClaim(TerminateMileageClaimRequest request, StreamObserver<TerminateMileageClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.terminateMileageClaim(request));
    }

    /**
     * Renews a mileage claim.
     */
    @Override
    public void renewMileageClaim(RenewMileageClaimRequest request, StreamObserver<RenewMileageClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.renewMileageClaim(request));
    }

    /**
     * Expires a mileage claim.
     */
    @Override
    public void expireMileageClaim(ExpireMileageClaimRequest request, StreamObserver<ExpireMileageClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.expireMileageClaim(request));
    }

    /**
     * Returns a mileage claim.
     */
    @Override
    public void getMileageClaimLifecycle(GetMileageClaimLifecycleRequest request, StreamObserver<GetMileageClaimLifecycleResponse> observer) {
        RpcSupport.unary(observer, () -> application.getMileageClaimLifecycle(request));
    }

    /**
     * Schedules a mileage claim.
     */
    @Override
    public void scheduleMileageClaimExpiry(ScheduleMileageClaimExpiryRequest request, StreamObserver<ScheduleMileageClaimExpiryResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleMileageClaimExpiry(request));
    }
}
