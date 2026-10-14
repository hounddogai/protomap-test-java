package com.acme.finance.domains.procurement;

import com.acme.finance.platform.RpcSupport;
import com.acme.procurement.v1.ApproveRfqRequest;
import com.acme.procurement.v1.ApproveRfqResponse;
import com.acme.procurement.v1.CommentRfqApprovalRequest;
import com.acme.procurement.v1.CommentRfqApprovalResponse;
import com.acme.procurement.v1.DelegateRfqApprovalRequest;
import com.acme.procurement.v1.DelegateRfqApprovalResponse;
import com.acme.procurement.v1.EscalateRfqRequest;
import com.acme.procurement.v1.EscalateRfqResponse;
import com.acme.procurement.v1.GetRfqApprovalStatusRequest;
import com.acme.procurement.v1.GetRfqApprovalStatusResponse;
import com.acme.procurement.v1.ListPendingRfqsRequest;
import com.acme.procurement.v1.ListPendingRfqsResponse;
import com.acme.procurement.v1.RecallRfqRequest;
import com.acme.procurement.v1.RecallRfqResponse;
import com.acme.procurement.v1.RejectRfqRequest;
import com.acme.procurement.v1.RejectRfqResponse;
import com.acme.procurement.v1.RfqApprovalServiceGrpc;
import com.acme.procurement.v1.SubmitRfqRequest;
import com.acme.procurement.v1.SubmitRfqResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes rfqs through approval, with escalation and delegation.
 */
@GrpcService
public class RfqApprovalEndpoint extends RfqApprovalServiceGrpc.RfqApprovalServiceImplBase {
    protected final RfqApprovalApplication application;

    public RfqApprovalEndpoint(RfqApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a rfq.
     */
    @Override
    public void submitRfq(SubmitRfqRequest request, StreamObserver<SubmitRfqResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitRfq(request));
    }

    /**
     * Approves a rfq.
     */
    @Override
    public void approveRfq(ApproveRfqRequest request, StreamObserver<ApproveRfqResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveRfq(request));
    }

    /**
     * Rejects a rfq.
     */
    @Override
    public void rejectRfq(RejectRfqRequest request, StreamObserver<RejectRfqResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectRfq(request));
    }

    /**
     * Escalates a rfq.
     */
    @Override
    public void escalateRfq(EscalateRfqRequest request, StreamObserver<EscalateRfqResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateRfq(request));
    }

    /**
     * Lists rfqs a page at a time.
     */
    @Override
    public void listPendingRfqs(ListPendingRfqsRequest request, StreamObserver<ListPendingRfqsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingRfqs(request));
    }

    /**
     * Delegates a rfq.
     */
    @Override
    public void delegateRfqApproval(DelegateRfqApprovalRequest request, StreamObserver<DelegateRfqApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateRfqApproval(request));
    }

    /**
     * Recalls a rfq.
     */
    @Override
    public void recallRfq(RecallRfqRequest request, StreamObserver<RecallRfqResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallRfq(request));
    }

    /**
     * Returns a rfq.
     */
    @Override
    public void getRfqApprovalStatus(GetRfqApprovalStatusRequest request, StreamObserver<GetRfqApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getRfqApprovalStatus(request));
    }

    /**
     * Comments a rfq.
     */
    @Override
    public void commentRfqApproval(CommentRfqApprovalRequest request, StreamObserver<CommentRfqApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentRfqApproval(request));
    }
}
