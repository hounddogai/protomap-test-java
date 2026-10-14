package com.acme.finance.domains.contracts;

import com.acme.contracts.v1.ApproveContractRequest;
import com.acme.contracts.v1.ApproveContractResponse;
import com.acme.contracts.v1.ContractApprovalServiceGrpc;
import com.acme.contracts.v1.DelegateContractApprovalRequest;
import com.acme.contracts.v1.DelegateContractApprovalResponse;
import com.acme.contracts.v1.EscalateContractRequest;
import com.acme.contracts.v1.EscalateContractResponse;
import com.acme.contracts.v1.GetContractApprovalStatusRequest;
import com.acme.contracts.v1.GetContractApprovalStatusResponse;
import com.acme.contracts.v1.ListPendingContractsRequest;
import com.acme.contracts.v1.ListPendingContractsResponse;
import com.acme.contracts.v1.RecallContractRequest;
import com.acme.contracts.v1.RecallContractResponse;
import com.acme.contracts.v1.RejectContractRequest;
import com.acme.contracts.v1.RejectContractResponse;
import com.acme.contracts.v1.SubmitContractRequest;
import com.acme.contracts.v1.SubmitContractResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes contracts through approval, with escalation and delegation.
 */
@GrpcService
public class ContractApprovalEndpoint extends ContractApprovalServiceGrpc.ContractApprovalServiceImplBase {
    protected final ContractApprovalApplication application;

    public ContractApprovalEndpoint(ContractApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a contract.
     */
    @Override
    public void submitContract(SubmitContractRequest request, StreamObserver<SubmitContractResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitContract(request));
    }

    /**
     * Approves a contract.
     */
    @Override
    public void approveContract(ApproveContractRequest request, StreamObserver<ApproveContractResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveContract(request));
    }

    /**
     * Rejects a contract.
     */
    @Override
    public void rejectContract(RejectContractRequest request, StreamObserver<RejectContractResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectContract(request));
    }

    /**
     * Escalates a contract.
     */
    @Override
    public void escalateContract(EscalateContractRequest request, StreamObserver<EscalateContractResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateContract(request));
    }

    /**
     * Lists contracts a page at a time.
     */
    @Override
    public void listPendingContracts(ListPendingContractsRequest request, StreamObserver<ListPendingContractsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingContracts(request));
    }

    /**
     * Delegates a contract.
     */
    @Override
    public void delegateContractApproval(DelegateContractApprovalRequest request, StreamObserver<DelegateContractApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateContractApproval(request));
    }

    /**
     * Recalls a contract.
     */
    @Override
    public void recallContract(RecallContractRequest request, StreamObserver<RecallContractResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallContract(request));
    }

    /**
     * Returns a contract.
     */
    @Override
    public void getContractApprovalStatus(GetContractApprovalStatusRequest request, StreamObserver<GetContractApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getContractApprovalStatus(request));
    }
}
