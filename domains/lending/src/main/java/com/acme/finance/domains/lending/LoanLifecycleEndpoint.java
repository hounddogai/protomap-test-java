package com.acme.finance.domains.lending;

import com.acme.finance.platform.RpcSupport;
import com.acme.lending.v1.ActivateLoanRequest;
import com.acme.lending.v1.ActivateLoanResponse;
import com.acme.lending.v1.GetLoanLifecycleRequest;
import com.acme.lending.v1.GetLoanLifecycleResponse;
import com.acme.lending.v1.LoanLifecycleServiceGrpc;
import com.acme.lending.v1.ReinstateLoanRequest;
import com.acme.lending.v1.ReinstateLoanResponse;
import com.acme.lending.v1.RenewLoanRequest;
import com.acme.lending.v1.RenewLoanResponse;
import com.acme.lending.v1.ScheduleLoanExpiryRequest;
import com.acme.lending.v1.ScheduleLoanExpiryResponse;
import com.acme.lending.v1.SuspendLoanRequest;
import com.acme.lending.v1.SuspendLoanResponse;
import com.acme.lending.v1.TerminateLoanRequest;
import com.acme.lending.v1.TerminateLoanResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Moves a loan between the states of its lifecycle.
 */
@GrpcService
public class LoanLifecycleEndpoint extends LoanLifecycleServiceGrpc.LoanLifecycleServiceImplBase {
    protected final LoanLifecycleApplication application;

    public LoanLifecycleEndpoint(LoanLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates a loan.
     */
    @Override
    public void activateLoan(ActivateLoanRequest request, StreamObserver<ActivateLoanResponse> observer) {
        RpcSupport.unary(observer, () -> application.activateLoan(request));
    }

    /**
     * Suspends a loan.
     */
    @Override
    public void suspendLoan(SuspendLoanRequest request, StreamObserver<SuspendLoanResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendLoan(request));
    }

    /**
     * Reinstates a loan.
     */
    @Override
    public void reinstateLoan(ReinstateLoanRequest request, StreamObserver<ReinstateLoanResponse> observer) {
        RpcSupport.unary(observer, () -> application.reinstateLoan(request));
    }

    /**
     * Terminates a loan.
     */
    @Override
    public void terminateLoan(TerminateLoanRequest request, StreamObserver<TerminateLoanResponse> observer) {
        RpcSupport.unary(observer, () -> application.terminateLoan(request));
    }

    /**
     * Renews a loan.
     */
    @Override
    public void renewLoan(RenewLoanRequest request, StreamObserver<RenewLoanResponse> observer) {
        RpcSupport.unary(observer, () -> application.renewLoan(request));
    }

    /**
     * Returns a loan.
     */
    @Override
    public void getLoanLifecycle(GetLoanLifecycleRequest request, StreamObserver<GetLoanLifecycleResponse> observer) {
        RpcSupport.unary(observer, () -> application.getLoanLifecycle(request));
    }

    /**
     * Schedules a loan.
     */
    @Override
    public void scheduleLoanExpiry(ScheduleLoanExpiryRequest request, StreamObserver<ScheduleLoanExpiryResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleLoanExpiry(request));
    }
}
