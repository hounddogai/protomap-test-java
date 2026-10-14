package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.AllocationCalculationServiceGrpc;
import com.acme.accounting.v1.CalculateAllocationRequest;
import com.acme.accounting.v1.CalculateAllocationResponse;
import com.acme.accounting.v1.CompareAllocationOptionsRequest;
import com.acme.accounting.v1.CompareAllocationOptionsResponse;
import com.acme.accounting.v1.EstimateAllocationRequest;
import com.acme.accounting.v1.EstimateAllocationResponse;
import com.acme.accounting.v1.GetAllocationRatesRequest;
import com.acme.accounting.v1.GetAllocationRatesResponse;
import com.acme.accounting.v1.RecalculateAllocationsRequest;
import com.acme.accounting.v1.RecalculateAllocationsResponse;
import com.acme.accounting.v1.SimulateAllocationRequest;
import com.acme.accounting.v1.SimulateAllocationResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Computes the figures of an allocation, such as totals, taxes, and estimates.
 */
public abstract class AbstractAllocationCalculationEndpoint extends AllocationCalculationServiceGrpc.AllocationCalculationServiceImplBase {
    protected final AllocationCalculationApplication application;

    protected AbstractAllocationCalculationEndpoint(AllocationCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates an allocation.
     */
    @Override
    public void calculateAllocation(CalculateAllocationRequest request, StreamObserver<CalculateAllocationResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateAllocation(request));
    }

    /**
     * Estimates an allocation.
     */
    @Override
    public void estimateAllocation(EstimateAllocationRequest request, StreamObserver<EstimateAllocationResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateAllocation(request));
    }

    /**
     * Compares an allocation.
     */
    @Override
    public void compareAllocationOptions(CompareAllocationOptionsRequest request, StreamObserver<CompareAllocationOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareAllocationOptions(request));
    }

    /**
     * Recalculates an allocation.
     */
    @Override
    public void recalculateAllocations(RecalculateAllocationsRequest request, StreamObserver<RecalculateAllocationsResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateAllocations(request));
    }

    /**
     * Simulates an allocation.
     */
    @Override
    public void simulateAllocation(SimulateAllocationRequest request, StreamObserver<SimulateAllocationResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateAllocation(request));
    }

    /**
     * Returns an allocation.
     */
    @Override
    public void getAllocationRates(GetAllocationRatesRequest request, StreamObserver<GetAllocationRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getAllocationRates(request));
    }
}
