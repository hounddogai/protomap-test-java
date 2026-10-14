package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.ApproveForecastRequest;
import com.acme.budgeting.v1.ApproveForecastResponse;
import com.acme.budgeting.v1.CommentForecastApprovalRequest;
import com.acme.budgeting.v1.CommentForecastApprovalResponse;
import com.acme.budgeting.v1.DelegateForecastApprovalRequest;
import com.acme.budgeting.v1.DelegateForecastApprovalResponse;
import com.acme.budgeting.v1.ForecastApprovalServiceGrpc;
import com.acme.budgeting.v1.GetForecastApprovalStatusRequest;
import com.acme.budgeting.v1.GetForecastApprovalStatusResponse;
import com.acme.budgeting.v1.ListPendingForecastsRequest;
import com.acme.budgeting.v1.ListPendingForecastsResponse;
import com.acme.budgeting.v1.RecallForecastRequest;
import com.acme.budgeting.v1.RecallForecastResponse;
import com.acme.budgeting.v1.RejectForecastRequest;
import com.acme.budgeting.v1.RejectForecastResponse;
import com.acme.budgeting.v1.SubmitForecastRequest;
import com.acme.budgeting.v1.SubmitForecastResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes forecasts through approval, with escalation and delegation.
 */
@GrpcService
public class ForecastApprovalEndpoint extends ForecastApprovalServiceGrpc.ForecastApprovalServiceImplBase {
    protected final ForecastApprovalApplication application;

    public ForecastApprovalEndpoint(ForecastApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a forecast.
     */
    @Override
    public void submitForecast(SubmitForecastRequest request, StreamObserver<SubmitForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitForecast(request));
    }

    /**
     * Approves a forecast.
     */
    @Override
    public void approveForecast(ApproveForecastRequest request, StreamObserver<ApproveForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveForecast(request));
    }

    /**
     * Rejects a forecast.
     */
    @Override
    public void rejectForecast(RejectForecastRequest request, StreamObserver<RejectForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectForecast(request));
    }

    /**
     * Lists forecasts a page at a time.
     */
    @Override
    public void listPendingForecasts(ListPendingForecastsRequest request, StreamObserver<ListPendingForecastsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingForecasts(request));
    }

    /**
     * Delegates a forecast.
     */
    @Override
    public void delegateForecastApproval(DelegateForecastApprovalRequest request, StreamObserver<DelegateForecastApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateForecastApproval(request));
    }

    /**
     * Recalls a forecast.
     */
    @Override
    public void recallForecast(RecallForecastRequest request, StreamObserver<RecallForecastResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallForecast(request));
    }

    /**
     * Returns a forecast.
     */
    @Override
    public void getForecastApprovalStatus(GetForecastApprovalStatusRequest request, StreamObserver<GetForecastApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getForecastApprovalStatus(request));
    }

    /**
     * Comments a forecast.
     */
    @Override
    public void commentForecastApproval(CommentForecastApprovalRequest request, StreamObserver<CommentForecastApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentForecastApproval(request));
    }
}
