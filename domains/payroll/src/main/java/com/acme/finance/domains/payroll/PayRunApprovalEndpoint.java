package com.acme.finance.domains.payroll;

import com.acme.finance.platform.RpcSupport;
import com.acme.payroll.v1.ApprovePayRunRequest;
import com.acme.payroll.v1.ApprovePayRunResponse;
import com.acme.payroll.v1.DelegatePayRunApprovalRequest;
import com.acme.payroll.v1.DelegatePayRunApprovalResponse;
import com.acme.payroll.v1.EscalatePayRunRequest;
import com.acme.payroll.v1.EscalatePayRunResponse;
import com.acme.payroll.v1.GetPayRunApprovalStatusRequest;
import com.acme.payroll.v1.GetPayRunApprovalStatusResponse;
import com.acme.payroll.v1.ListPendingPayRunsRequest;
import com.acme.payroll.v1.ListPendingPayRunsResponse;
import com.acme.payroll.v1.PayRunApprovalServiceGrpc;
import com.acme.payroll.v1.RecallPayRunRequest;
import com.acme.payroll.v1.RecallPayRunResponse;
import com.acme.payroll.v1.RejectPayRunRequest;
import com.acme.payroll.v1.RejectPayRunResponse;
import com.acme.payroll.v1.SubmitPayRunRequest;
import com.acme.payroll.v1.SubmitPayRunResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes pay runs through approval, with escalation and delegation.
 */
@GrpcService
public class PayRunApprovalEndpoint extends PayRunApprovalServiceGrpc.PayRunApprovalServiceImplBase {
    protected final PayRunApprovalApplication application;

    public PayRunApprovalEndpoint(PayRunApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a pay run.
     */
    @Override
    public void submitPayRun(SubmitPayRunRequest request, StreamObserver<SubmitPayRunResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitPayRun(request));
    }

    /**
     * Approves a pay run.
     */
    @Override
    public void approvePayRun(ApprovePayRunRequest request, StreamObserver<ApprovePayRunResponse> observer) {
        RpcSupport.unary(observer, () -> application.approvePayRun(request));
    }

    /**
     * Rejects a pay run.
     */
    @Override
    public void rejectPayRun(RejectPayRunRequest request, StreamObserver<RejectPayRunResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectPayRun(request));
    }

    /**
     * Escalates a pay run.
     */
    @Override
    public void escalatePayRun(EscalatePayRunRequest request, StreamObserver<EscalatePayRunResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalatePayRun(request));
    }

    /**
     * Lists pay runs a page at a time.
     */
    @Override
    public void listPendingPayRuns(ListPendingPayRunsRequest request, StreamObserver<ListPendingPayRunsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingPayRuns(request));
    }

    /**
     * Delegates a pay run.
     */
    @Override
    public void delegatePayRunApproval(DelegatePayRunApprovalRequest request, StreamObserver<DelegatePayRunApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegatePayRunApproval(request));
    }

    /**
     * Recalls a pay run.
     */
    @Override
    public void recallPayRun(RecallPayRunRequest request, StreamObserver<RecallPayRunResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallPayRun(request));
    }

    /**
     * Returns a pay run.
     */
    @Override
    public void getPayRunApprovalStatus(GetPayRunApprovalStatusRequest request, StreamObserver<GetPayRunApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getPayRunApprovalStatus(request));
    }
}
