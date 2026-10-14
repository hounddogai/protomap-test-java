package com.acme.finance.domains.billing;

import com.acme.billing.v1.ApproveBillingAccountRequest;
import com.acme.billing.v1.ApproveBillingAccountResponse;
import com.acme.billing.v1.BillingAccountApprovalServiceGrpc;
import com.acme.billing.v1.CommentBillingAccountApprovalRequest;
import com.acme.billing.v1.CommentBillingAccountApprovalResponse;
import com.acme.billing.v1.DelegateBillingAccountApprovalRequest;
import com.acme.billing.v1.DelegateBillingAccountApprovalResponse;
import com.acme.billing.v1.EscalateBillingAccountRequest;
import com.acme.billing.v1.EscalateBillingAccountResponse;
import com.acme.billing.v1.ListPendingBillingAccountsRequest;
import com.acme.billing.v1.ListPendingBillingAccountsResponse;
import com.acme.billing.v1.RecallBillingAccountRequest;
import com.acme.billing.v1.RecallBillingAccountResponse;
import com.acme.billing.v1.RejectBillingAccountRequest;
import com.acme.billing.v1.RejectBillingAccountResponse;
import com.acme.billing.v1.SubmitBillingAccountRequest;
import com.acme.billing.v1.SubmitBillingAccountResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes billing accounts through approval, with escalation and delegation.
 */
@GrpcService
public class BillingAccountApprovalEndpoint extends BillingAccountApprovalServiceGrpc.BillingAccountApprovalServiceImplBase {
    protected final BillingAccountApprovalApplication application;

    public BillingAccountApprovalEndpoint(BillingAccountApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a billing account.
     */
    @Override
    public void submitBillingAccount(SubmitBillingAccountRequest request, StreamObserver<SubmitBillingAccountResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitBillingAccount(request));
    }

    /**
     * Approves a billing account.
     */
    @Override
    public void approveBillingAccount(ApproveBillingAccountRequest request, StreamObserver<ApproveBillingAccountResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveBillingAccount(request));
    }

    /**
     * Rejects a billing account.
     */
    @Override
    public void rejectBillingAccount(RejectBillingAccountRequest request, StreamObserver<RejectBillingAccountResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectBillingAccount(request));
    }

    /**
     * Escalates a billing account.
     */
    @Override
    public void escalateBillingAccount(EscalateBillingAccountRequest request, StreamObserver<EscalateBillingAccountResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateBillingAccount(request));
    }

    /**
     * Lists billing accounts a page at a time.
     */
    @Override
    public void listPendingBillingAccounts(ListPendingBillingAccountsRequest request, StreamObserver<ListPendingBillingAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingBillingAccounts(request));
    }

    /**
     * Delegates a billing account.
     */
    @Override
    public void delegateBillingAccountApproval(DelegateBillingAccountApprovalRequest request, StreamObserver<DelegateBillingAccountApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateBillingAccountApproval(request));
    }

    /**
     * Recalls a billing account.
     */
    @Override
    public void recallBillingAccount(RecallBillingAccountRequest request, StreamObserver<RecallBillingAccountResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallBillingAccount(request));
    }

    /**
     * Comments a billing account.
     */
    @Override
    public void commentBillingAccountApproval(CommentBillingAccountApprovalRequest request, StreamObserver<CommentBillingAccountApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentBillingAccountApproval(request));
    }
}
