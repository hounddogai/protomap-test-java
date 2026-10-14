package com.acme.finance.domains.collections;

import com.acme.collections.v1.ApproveWriteOffRequest;
import com.acme.collections.v1.ApproveWriteOffResponse;
import com.acme.collections.v1.CommentWriteOffApprovalRequest;
import com.acme.collections.v1.CommentWriteOffApprovalResponse;
import com.acme.collections.v1.EscalateWriteOffRequest;
import com.acme.collections.v1.EscalateWriteOffResponse;
import com.acme.collections.v1.ListPendingWriteOffsRequest;
import com.acme.collections.v1.ListPendingWriteOffsResponse;
import com.acme.collections.v1.RecallWriteOffRequest;
import com.acme.collections.v1.RecallWriteOffResponse;
import com.acme.collections.v1.RejectWriteOffRequest;
import com.acme.collections.v1.RejectWriteOffResponse;
import com.acme.collections.v1.SubmitWriteOffRequest;
import com.acme.collections.v1.SubmitWriteOffResponse;
import com.acme.collections.v1.WriteOffApprovalServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes write offs through approval, with escalation and delegation.
 */
@GrpcService
public class WriteOffApprovalEndpoint extends WriteOffApprovalServiceGrpc.WriteOffApprovalServiceImplBase {
    protected final WriteOffApprovalApplication application;

    public WriteOffApprovalEndpoint(WriteOffApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a write off.
     */
    @Override
    public void submitWriteOff(SubmitWriteOffRequest request, StreamObserver<SubmitWriteOffResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitWriteOff(request));
    }

    /**
     * Approves a write off.
     */
    @Override
    public void approveWriteOff(ApproveWriteOffRequest request, StreamObserver<ApproveWriteOffResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveWriteOff(request));
    }

    /**
     * Rejects a write off.
     */
    @Override
    public void rejectWriteOff(RejectWriteOffRequest request, StreamObserver<RejectWriteOffResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectWriteOff(request));
    }

    /**
     * Escalates a write off.
     */
    @Override
    public void escalateWriteOff(EscalateWriteOffRequest request, StreamObserver<EscalateWriteOffResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateWriteOff(request));
    }

    /**
     * Lists write offs a page at a time.
     */
    @Override
    public void listPendingWriteOffs(ListPendingWriteOffsRequest request, StreamObserver<ListPendingWriteOffsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingWriteOffs(request));
    }

    /**
     * Recalls a write off.
     */
    @Override
    public void recallWriteOff(RecallWriteOffRequest request, StreamObserver<RecallWriteOffResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallWriteOff(request));
    }

    /**
     * Comments a write off.
     */
    @Override
    public void commentWriteOffApproval(CommentWriteOffApprovalRequest request, StreamObserver<CommentWriteOffApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentWriteOffApproval(request));
    }
}
