package com.acme.finance.domains.treasury;

import com.acme.finance.platform.RpcSupport;
import com.acme.treasury.v1.ApproveLiquidityForecastRequest;
import com.acme.treasury.v1.ApproveLiquidityForecastResponse;
import com.acme.treasury.v1.CommentLiquidityForecastApprovalRequest;
import com.acme.treasury.v1.CommentLiquidityForecastApprovalResponse;
import com.acme.treasury.v1.DelegateLiquidityForecastApprovalRequest;
import com.acme.treasury.v1.DelegateLiquidityForecastApprovalResponse;
import com.acme.treasury.v1.EscalateLiquidityForecastRequest;
import com.acme.treasury.v1.EscalateLiquidityForecastResponse;
import com.acme.treasury.v1.GetLiquidityForecastApprovalStatusRequest;
import com.acme.treasury.v1.GetLiquidityForecastApprovalStatusResponse;
import com.acme.treasury.v1.LiquidityForecastApprovalServiceGrpc;
import com.acme.treasury.v1.RecallLiquidityForecastRequest;
import com.acme.treasury.v1.RecallLiquidityForecastResponse;
import com.acme.treasury.v1.RejectLiquidityForecastRequest;
import com.acme.treasury.v1.RejectLiquidityForecastResponse;
import com.acme.treasury.v1.SubmitLiquidityForecastRequest;
import com.acme.treasury.v1.SubmitLiquidityForecastResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes liquidity forecasts through approval, with escalation and delegation.
 */
@GrpcService
public class LiquidityForecastApprovalEndpoint extends LiquidityForecastApprovalServiceGrpc.LiquidityForecastApprovalServiceImplBase {
    protected final LiquidityForecastApprovalApplication application;

    public LiquidityForecastApprovalEndpoint(LiquidityForecastApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a liquidity forecast.
     */
    @Override
    public void submitLiquidityForecast(SubmitLiquidityForecastRequest request, StreamObserver<SubmitLiquidityForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitLiquidityForecast(request));
    }

    /**
     * Approves a liquidity forecast.
     */
    @Override
    public void approveLiquidityForecast(ApproveLiquidityForecastRequest request, StreamObserver<ApproveLiquidityForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveLiquidityForecast(request));
    }

    /**
     * Rejects a liquidity forecast.
     */
    @Override
    public void rejectLiquidityForecast(RejectLiquidityForecastRequest request, StreamObserver<RejectLiquidityForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectLiquidityForecast(request));
    }

    /**
     * Escalates a liquidity forecast.
     */
    @Override
    public void escalateLiquidityForecast(EscalateLiquidityForecastRequest request, StreamObserver<EscalateLiquidityForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateLiquidityForecast(request));
    }

    /**
     * Delegates a liquidity forecast.
     */
    @Override
    public void delegateLiquidityForecastApproval(DelegateLiquidityForecastApprovalRequest request, StreamObserver<DelegateLiquidityForecastApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateLiquidityForecastApproval(request));
    }

    /**
     * Recalls a liquidity forecast.
     */
    @Override
    public void recallLiquidityForecast(RecallLiquidityForecastRequest request, StreamObserver<RecallLiquidityForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallLiquidityForecast(request));
    }

    /**
     * Returns a liquidity forecast.
     */
    @Override
    public void getLiquidityForecastApprovalStatus(GetLiquidityForecastApprovalStatusRequest request, StreamObserver<GetLiquidityForecastApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getLiquidityForecastApprovalStatus(request));
    }

    /**
     * Comments a liquidity forecast.
     */
    @Override
    public void commentLiquidityForecastApproval(CommentLiquidityForecastApprovalRequest request, StreamObserver<CommentLiquidityForecastApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentLiquidityForecastApproval(request));
    }
}
