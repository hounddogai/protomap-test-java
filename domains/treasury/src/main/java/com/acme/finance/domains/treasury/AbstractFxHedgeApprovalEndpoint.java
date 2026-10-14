package com.acme.finance.domains.treasury;

import com.acme.finance.platform.RpcSupport;
import com.acme.treasury.v1.ApproveFxHedgeRequest;
import com.acme.treasury.v1.ApproveFxHedgeResponse;
import com.acme.treasury.v1.CommentFxHedgeApprovalRequest;
import com.acme.treasury.v1.CommentFxHedgeApprovalResponse;
import com.acme.treasury.v1.EscalateFxHedgeRequest;
import com.acme.treasury.v1.EscalateFxHedgeResponse;
import com.acme.treasury.v1.FxHedgeApprovalServiceGrpc;
import com.acme.treasury.v1.GetFxHedgeApprovalStatusRequest;
import com.acme.treasury.v1.GetFxHedgeApprovalStatusResponse;
import com.acme.treasury.v1.ListPendingFxHedgesRequest;
import com.acme.treasury.v1.ListPendingFxHedgesResponse;
import com.acme.treasury.v1.SubmitFxHedgeRequest;
import com.acme.treasury.v1.SubmitFxHedgeResponse;
import io.grpc.stub.StreamObserver;

/**
 * Routes fx hedges through approval, with escalation and delegation.
 */
public abstract class AbstractFxHedgeApprovalEndpoint extends FxHedgeApprovalServiceGrpc.FxHedgeApprovalServiceImplBase {
    protected final FxHedgeApprovalApplication application;

    protected AbstractFxHedgeApprovalEndpoint(FxHedgeApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a fx hedge.
     */
    @Override
    public void submitFxHedge(SubmitFxHedgeRequest request, StreamObserver<SubmitFxHedgeResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitFxHedge(request));
    }

    /**
     * Approves a fx hedge.
     */
    @Override
    public void approveFxHedge(ApproveFxHedgeRequest request, StreamObserver<ApproveFxHedgeResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveFxHedge(request));
    }

    /**
     * Escalates a fx hedge.
     */
    @Override
    public void escalateFxHedge(EscalateFxHedgeRequest request, StreamObserver<EscalateFxHedgeResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateFxHedge(request));
    }

    /**
     * Lists fx hedges a page at a time.
     */
    @Override
    public void listPendingFxHedges(ListPendingFxHedgesRequest request, StreamObserver<ListPendingFxHedgesResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingFxHedges(request));
    }

    /**
     * Returns a fx hedge.
     */
    @Override
    public void getFxHedgeApprovalStatus(GetFxHedgeApprovalStatusRequest request, StreamObserver<GetFxHedgeApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getFxHedgeApprovalStatus(request));
    }

    /**
     * Comments a fx hedge.
     */
    @Override
    public void commentFxHedgeApproval(CommentFxHedgeApprovalRequest request, StreamObserver<CommentFxHedgeApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentFxHedgeApproval(request));
    }
}
