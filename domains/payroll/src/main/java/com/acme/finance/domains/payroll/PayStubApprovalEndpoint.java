package com.acme.finance.domains.payroll;

import com.acme.finance.platform.RpcSupport;
import com.acme.payroll.v1.ApprovePayStubRequest;
import com.acme.payroll.v1.ApprovePayStubResponse;
import com.acme.payroll.v1.CommentPayStubApprovalRequest;
import com.acme.payroll.v1.CommentPayStubApprovalResponse;
import com.acme.payroll.v1.DelegatePayStubApprovalRequest;
import com.acme.payroll.v1.DelegatePayStubApprovalResponse;
import com.acme.payroll.v1.EscalatePayStubRequest;
import com.acme.payroll.v1.EscalatePayStubResponse;
import com.acme.payroll.v1.GetPayStubApprovalStatusRequest;
import com.acme.payroll.v1.GetPayStubApprovalStatusResponse;
import com.acme.payroll.v1.ListPendingPayStubsRequest;
import com.acme.payroll.v1.ListPendingPayStubsResponse;
import com.acme.payroll.v1.PayStubApprovalServiceGrpc;
import com.acme.payroll.v1.RecallPayStubRequest;
import com.acme.payroll.v1.RecallPayStubResponse;
import com.acme.payroll.v1.RejectPayStubRequest;
import com.acme.payroll.v1.RejectPayStubResponse;
import com.acme.payroll.v1.SubmitPayStubRequest;
import com.acme.payroll.v1.SubmitPayStubResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes pay stubs through approval, with escalation and delegation.
 */
@GrpcService
public class PayStubApprovalEndpoint extends PayStubApprovalServiceGrpc.PayStubApprovalServiceImplBase {
    protected final PayStubApprovalApplication application;

    public PayStubApprovalEndpoint(PayStubApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a pay stub.
     */
    @Override
    public void submitPayStub(SubmitPayStubRequest request, StreamObserver<SubmitPayStubResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitPayStub(request));
    }

    /**
     * Approves a pay stub.
     */
    @Override
    public void approvePayStub(ApprovePayStubRequest request, StreamObserver<ApprovePayStubResponse> observer) {
        RpcSupport.unary(observer, () -> application.approvePayStub(request));
    }

    /**
     * Rejects a pay stub.
     */
    @Override
    public void rejectPayStub(RejectPayStubRequest request, StreamObserver<RejectPayStubResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectPayStub(request));
    }

    /**
     * Escalates a pay stub.
     */
    @Override
    public void escalatePayStub(EscalatePayStubRequest request, StreamObserver<EscalatePayStubResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalatePayStub(request));
    }

    /**
     * Lists pay stubs a page at a time.
     */
    @Override
    public void listPendingPayStubs(ListPendingPayStubsRequest request, StreamObserver<ListPendingPayStubsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingPayStubs(request));
    }

    /**
     * Delegates a pay stub.
     */
    @Override
    public void delegatePayStubApproval(DelegatePayStubApprovalRequest request, StreamObserver<DelegatePayStubApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegatePayStubApproval(request));
    }

    /**
     * Recalls a pay stub.
     */
    @Override
    public void recallPayStub(RecallPayStubRequest request, StreamObserver<RecallPayStubResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallPayStub(request));
    }

    /**
     * Returns a pay stub.
     */
    @Override
    public void getPayStubApprovalStatus(GetPayStubApprovalStatusRequest request, StreamObserver<GetPayStubApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getPayStubApprovalStatus(request));
    }

    /**
     * Comments a pay stub.
     */
    @Override
    public void commentPayStubApproval(CommentPayStubApprovalRequest request, StreamObserver<CommentPayStubApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentPayStubApproval(request));
    }
}
