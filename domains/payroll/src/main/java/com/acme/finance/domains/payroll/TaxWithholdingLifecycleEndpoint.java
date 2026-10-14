package com.acme.finance.domains.payroll;

import com.acme.finance.platform.RpcSupport;
import com.acme.payroll.v1.ActivateTaxWithholdingRequest;
import com.acme.payroll.v1.ActivateTaxWithholdingResponse;
import com.acme.payroll.v1.ExpireTaxWithholdingRequest;
import com.acme.payroll.v1.ExpireTaxWithholdingResponse;
import com.acme.payroll.v1.GetTaxWithholdingLifecycleRequest;
import com.acme.payroll.v1.GetTaxWithholdingLifecycleResponse;
import com.acme.payroll.v1.ReinstateTaxWithholdingRequest;
import com.acme.payroll.v1.ReinstateTaxWithholdingResponse;
import com.acme.payroll.v1.RenewTaxWithholdingRequest;
import com.acme.payroll.v1.RenewTaxWithholdingResponse;
import com.acme.payroll.v1.ScheduleTaxWithholdingExpiryRequest;
import com.acme.payroll.v1.ScheduleTaxWithholdingExpiryResponse;
import com.acme.payroll.v1.SuspendTaxWithholdingRequest;
import com.acme.payroll.v1.SuspendTaxWithholdingResponse;
import com.acme.payroll.v1.TaxWithholdingLifecycleServiceGrpc;
import com.acme.payroll.v1.TerminateTaxWithholdingRequest;
import com.acme.payroll.v1.TerminateTaxWithholdingResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Moves a tax withholding between the states of its lifecycle.
 */
@GrpcService
public class TaxWithholdingLifecycleEndpoint extends TaxWithholdingLifecycleServiceGrpc.TaxWithholdingLifecycleServiceImplBase {
    protected final TaxWithholdingLifecycleApplication application;

    public TaxWithholdingLifecycleEndpoint(TaxWithholdingLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates a tax withholding.
     */
    @Override
    public void activateTaxWithholding(ActivateTaxWithholdingRequest request, StreamObserver<ActivateTaxWithholdingResponse> observer) {
        RpcSupport.unary(observer, () -> application.activateTaxWithholding(request));
    }

    /**
     * Suspends a tax withholding.
     */
    @Override
    public void suspendTaxWithholding(SuspendTaxWithholdingRequest request, StreamObserver<SuspendTaxWithholdingResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendTaxWithholding(request));
    }

    /**
     * Reinstates a tax withholding.
     */
    @Override
    public void reinstateTaxWithholding(ReinstateTaxWithholdingRequest request, StreamObserver<ReinstateTaxWithholdingResponse> observer) {
        RpcSupport.unary(observer, () -> application.reinstateTaxWithholding(request));
    }

    /**
     * Terminates a tax withholding.
     */
    @Override
    public void terminateTaxWithholding(TerminateTaxWithholdingRequest request, StreamObserver<TerminateTaxWithholdingResponse> observer) {
        RpcSupport.unary(observer, () -> application.terminateTaxWithholding(request));
    }

    /**
     * Renews a tax withholding.
     */
    @Override
    public void renewTaxWithholding(RenewTaxWithholdingRequest request, StreamObserver<RenewTaxWithholdingResponse> observer) {
        RpcSupport.unary(observer, () -> application.renewTaxWithholding(request));
    }

    /**
     * Expires a tax withholding.
     */
    @Override
    public void expireTaxWithholding(ExpireTaxWithholdingRequest request, StreamObserver<ExpireTaxWithholdingResponse> observer) {
        RpcSupport.unary(observer, () -> application.expireTaxWithholding(request));
    }

    /**
     * Returns a tax withholding.
     */
    @Override
    public void getTaxWithholdingLifecycle(GetTaxWithholdingLifecycleRequest request, StreamObserver<GetTaxWithholdingLifecycleResponse> observer) {
        RpcSupport.unary(observer, () -> application.getTaxWithholdingLifecycle(request));
    }

    /**
     * Schedules a tax withholding.
     */
    @Override
    public void scheduleTaxWithholdingExpiry(ScheduleTaxWithholdingExpiryRequest request, StreamObserver<ScheduleTaxWithholdingExpiryResponse> observer) {
        RpcSupport.unary(observer, () -> application.scheduleTaxWithholdingExpiry(request));
    }
}
