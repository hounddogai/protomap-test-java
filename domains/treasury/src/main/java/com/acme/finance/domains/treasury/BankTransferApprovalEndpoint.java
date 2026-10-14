package com.acme.finance.domains.treasury;

import com.acme.finance.platform.RpcSupport;
import com.acme.treasury.v1.ApproveBankTransferRequest;
import com.acme.treasury.v1.ApproveBankTransferResponse;
import com.acme.treasury.v1.BankTransferApprovalServiceGrpc;
import com.acme.treasury.v1.CommentBankTransferApprovalRequest;
import com.acme.treasury.v1.CommentBankTransferApprovalResponse;
import com.acme.treasury.v1.DelegateBankTransferApprovalRequest;
import com.acme.treasury.v1.DelegateBankTransferApprovalResponse;
import com.acme.treasury.v1.EscalateBankTransferRequest;
import com.acme.treasury.v1.EscalateBankTransferResponse;
import com.acme.treasury.v1.ListPendingBankTransfersRequest;
import com.acme.treasury.v1.ListPendingBankTransfersResponse;
import com.acme.treasury.v1.SubmitBankTransferRequest;
import com.acme.treasury.v1.SubmitBankTransferResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes bank transfers through approval, with escalation and delegation.
 */
@GrpcService
public class BankTransferApprovalEndpoint extends BankTransferApprovalServiceGrpc.BankTransferApprovalServiceImplBase {
    protected final BankTransferApprovalApplication application;

    public BankTransferApprovalEndpoint(BankTransferApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a bank transfer.
     */
    @Override
    public void submitBankTransfer(SubmitBankTransferRequest request, StreamObserver<SubmitBankTransferResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitBankTransfer(request));
    }

    /**
     * Approves a bank transfer.
     */
    @Override
    public void approveBankTransfer(ApproveBankTransferRequest request, StreamObserver<ApproveBankTransferResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveBankTransfer(request));
    }

    /**
     * Escalates a bank transfer.
     */
    @Override
    public void escalateBankTransfer(EscalateBankTransferRequest request, StreamObserver<EscalateBankTransferResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateBankTransfer(request));
    }

    /**
     * Lists bank transfers a page at a time.
     */
    @Override
    public void listPendingBankTransfers(ListPendingBankTransfersRequest request, StreamObserver<ListPendingBankTransfersResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingBankTransfers(request));
    }

    /**
     * Delegates a bank transfer.
     */
    @Override
    public void delegateBankTransferApproval(DelegateBankTransferApprovalRequest request, StreamObserver<DelegateBankTransferApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateBankTransferApproval(request));
    }

    /**
     * Comments a bank transfer.
     */
    @Override
    public void commentBankTransferApproval(CommentBankTransferApprovalRequest request, StreamObserver<CommentBankTransferApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentBankTransferApproval(request));
    }
}
