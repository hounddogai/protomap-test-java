package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.AllocationApprovalServiceGrpc;
import com.acme.accounting.v1.ApproveAllocationRequest;
import com.acme.accounting.v1.ApproveAllocationResponse;
import com.acme.accounting.v1.CommentAllocationApprovalRequest;
import com.acme.accounting.v1.CommentAllocationApprovalResponse;
import com.acme.accounting.v1.DelegateAllocationApprovalRequest;
import com.acme.accounting.v1.DelegateAllocationApprovalResponse;
import com.acme.accounting.v1.GetAllocationApprovalStatusRequest;
import com.acme.accounting.v1.GetAllocationApprovalStatusResponse;
import com.acme.accounting.v1.ListPendingAllocationsRequest;
import com.acme.accounting.v1.ListPendingAllocationsResponse;
import com.acme.accounting.v1.RecallAllocationRequest;
import com.acme.accounting.v1.RecallAllocationResponse;
import com.acme.accounting.v1.SubmitAllocationRequest;
import com.acme.accounting.v1.SubmitAllocationResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes allocations through approval, with escalation and delegation.
 */
@GrpcService
public class AllocationApprovalEndpoint extends AllocationApprovalServiceGrpc.AllocationApprovalServiceImplBase {
    protected final AllocationApprovalApplication application;

    public AllocationApprovalEndpoint(AllocationApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits an allocation.
     */
    @Override
    public void submitAllocation(SubmitAllocationRequest request, StreamObserver<SubmitAllocationResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitAllocation(request));
    }

    /**
     * Approves an allocation.
     */
    @Override
    public void approveAllocation(ApproveAllocationRequest request, StreamObserver<ApproveAllocationResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveAllocation(request));
    }

    /**
     * Lists allocations a page at a time.
     */
    @Override
    public void listPendingAllocations(ListPendingAllocationsRequest request, StreamObserver<ListPendingAllocationsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingAllocations(request));
    }

    /**
     * Delegates an allocation.
     */
    @Override
    public void delegateAllocationApproval(DelegateAllocationApprovalRequest request, StreamObserver<DelegateAllocationApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateAllocationApproval(request));
    }

    /**
     * Recalls an allocation.
     */
    @Override
    public void recallAllocation(RecallAllocationRequest request, StreamObserver<RecallAllocationResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallAllocation(request));
    }

    /**
     * Returns an allocation.
     */
    @Override
    public void getAllocationApprovalStatus(GetAllocationApprovalStatusRequest request, StreamObserver<GetAllocationApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getAllocationApprovalStatus(request));
    }

    /**
     * Comments an allocation.
     */
    @Override
    public void commentAllocationApproval(CommentAllocationApprovalRequest request, StreamObserver<CommentAllocationApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentAllocationApproval(request));
    }
}
