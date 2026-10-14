package com.acme.finance.domains.suppliers;

import com.acme.finance.platform.RpcSupport;
import com.acme.suppliers.v1.ApproveSupplierContractRequest;
import com.acme.suppliers.v1.ApproveSupplierContractResponse;
import com.acme.suppliers.v1.CommentSupplierContractApprovalRequest;
import com.acme.suppliers.v1.CommentSupplierContractApprovalResponse;
import com.acme.suppliers.v1.DelegateSupplierContractApprovalRequest;
import com.acme.suppliers.v1.DelegateSupplierContractApprovalResponse;
import com.acme.suppliers.v1.EscalateSupplierContractRequest;
import com.acme.suppliers.v1.EscalateSupplierContractResponse;
import com.acme.suppliers.v1.GetSupplierContractApprovalStatusRequest;
import com.acme.suppliers.v1.GetSupplierContractApprovalStatusResponse;
import com.acme.suppliers.v1.ListPendingSupplierContractsRequest;
import com.acme.suppliers.v1.ListPendingSupplierContractsResponse;
import com.acme.suppliers.v1.RecallSupplierContractRequest;
import com.acme.suppliers.v1.RecallSupplierContractResponse;
import com.acme.suppliers.v1.RejectSupplierContractRequest;
import com.acme.suppliers.v1.RejectSupplierContractResponse;
import com.acme.suppliers.v1.SubmitSupplierContractRequest;
import com.acme.suppliers.v1.SubmitSupplierContractResponse;
import com.acme.suppliers.v1.SupplierContractApprovalServiceGrpc;
import io.grpc.stub.StreamObserver;

/**
 * Routes supplier contracts through approval, with escalation and delegation.
 */
public abstract class AbstractSupplierContractApprovalEndpoint extends SupplierContractApprovalServiceGrpc.SupplierContractApprovalServiceImplBase {
    protected final SupplierContractApprovalApplication application;

    protected AbstractSupplierContractApprovalEndpoint(SupplierContractApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a supplier contract.
     */
    @Override
    public void submitSupplierContract(SubmitSupplierContractRequest request, StreamObserver<SubmitSupplierContractResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitSupplierContract(request));
    }

    /**
     * Approves a supplier contract.
     */
    @Override
    public void approveSupplierContract(ApproveSupplierContractRequest request, StreamObserver<ApproveSupplierContractResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveSupplierContract(request));
    }

    /**
     * Rejects a supplier contract.
     */
    @Override
    public void rejectSupplierContract(RejectSupplierContractRequest request, StreamObserver<RejectSupplierContractResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectSupplierContract(request));
    }

    /**
     * Escalates a supplier contract.
     */
    @Override
    public void escalateSupplierContract(EscalateSupplierContractRequest request, StreamObserver<EscalateSupplierContractResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateSupplierContract(request));
    }

    /**
     * Lists supplier contracts a page at a time.
     */
    @Override
    public void listPendingSupplierContracts(ListPendingSupplierContractsRequest request, StreamObserver<ListPendingSupplierContractsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingSupplierContracts(request));
    }

    /**
     * Delegates a supplier contract.
     */
    @Override
    public void delegateSupplierContractApproval(DelegateSupplierContractApprovalRequest request, StreamObserver<DelegateSupplierContractApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateSupplierContractApproval(request));
    }

    /**
     * Recalls a supplier contract.
     */
    @Override
    public void recallSupplierContract(RecallSupplierContractRequest request, StreamObserver<RecallSupplierContractResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallSupplierContract(request));
    }

    /**
     * Returns a supplier contract.
     */
    @Override
    public void getSupplierContractApprovalStatus(GetSupplierContractApprovalStatusRequest request, StreamObserver<GetSupplierContractApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getSupplierContractApprovalStatus(request));
    }

    /**
     * Comments a supplier contract.
     */
    @Override
    public void commentSupplierContractApproval(CommentSupplierContractApprovalRequest request, StreamObserver<CommentSupplierContractApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentSupplierContractApproval(request));
    }
}
