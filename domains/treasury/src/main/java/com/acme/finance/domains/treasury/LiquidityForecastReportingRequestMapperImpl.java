package com.acme.finance.domains.treasury;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailCardSettlement;
import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.audit.v1.PreviewAuditTrailRequest;
import com.acme.audit.v1.PreviewAuditTrailResponse;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineApprovalServiceGrpc;
import com.acme.budgeting.v1.EscalateBudgetLineRequest;
import com.acme.budgeting.v1.EscalateBudgetLineResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CompareHouseholdOptionsRequest;
import com.acme.customers.v1.CompareHouseholdOptionsResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.ExplainConsentRecordDecisionRequest;
import com.acme.customers.v1.ExplainConsentRecordDecisionResponse;
import com.acme.customers.v1.ExplainHouseholdDecisionRequest;
import com.acme.customers.v1.ExplainHouseholdDecisionResponse;
import com.acme.customers.v1.FindCustomersRequest;
import com.acme.customers.v1.FindCustomersResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.ScoreCustomerRequest;
import com.acme.customers.v1.ScoreCustomerResponse;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunBankSettlement;
import com.acme.payroll.v1.PayRunWorkflowServiceGrpc;
import com.acme.payroll.v1.RetryPayRunWorkflowRequest;
import com.acme.payroll.v1.RetryPayRunWorkflowResponse;
import com.acme.performance.v1.CreateReviewRequest;
import com.acme.performance.v1.CreateReviewResponse;
import com.acme.performance.v1.Review;
import com.acme.performance.v1.ReviewLine;
import com.acme.performance.v1.ReviewServiceGrpc;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderLifecycleServiceGrpc;
import com.acme.procurement.v1.SchedulePurchaseOrderExpiryRequest;
import com.acme.procurement.v1.SchedulePurchaseOrderExpiryResponse;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionServiceGrpc;
import com.acme.treasury.v1.CountLiquidityForecastTotalsRequest;
import com.acme.treasury.v1.CountLiquidityForecastTotalsResponse;
import com.acme.treasury.v1.DeleteLiquidityForecastReportRequest;
import com.acme.treasury.v1.DeleteLiquidityForecastReportResponse;
import com.acme.treasury.v1.FetchLiquidityForecastReportRequest;
import com.acme.treasury.v1.FetchLiquidityForecastReportResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeReportingServiceGrpc;
import com.acme.treasury.v1.FxHedgeSyncServiceGrpc;
import com.acme.treasury.v1.GenerateFxHedgeReportRequest;
import com.acme.treasury.v1.GenerateFxHedgeReportResponse;
import com.acme.treasury.v1.GenerateLiquidityForecastReportRequest;
import com.acme.treasury.v1.GenerateLiquidityForecastReportResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.LiquidityForecastCardSettlement;
import com.acme.treasury.v1.LiquidityForecastValidationServiceGrpc;
import com.acme.treasury.v1.ScheduleLiquidityForecastReportRequest;
import com.acme.treasury.v1.ScheduleLiquidityForecastReportResponse;
import com.acme.treasury.v1.ScoreLiquidityForecastRequest;
import com.acme.treasury.v1.ScoreLiquidityForecastResponse;
import com.acme.treasury.v1.ShareLiquidityForecastReportRequest;
import com.acme.treasury.v1.ShareLiquidityForecastReportResponse;
import com.acme.treasury.v1.StreamLiquidityForecastMetricsRequest;
import com.acme.treasury.v1.StreamLiquidityForecastMetricsResponse;
import com.acme.treasury.v1.SyncFxHedgesRequest;
import com.acme.treasury.v1.SyncFxHedgesResponse;
import com.acme.treasury.v1.UpdateCashPositionRequest;
import com.acme.treasury.v1.UpdateCashPositionResponse;
import com.google.protobuf.Any;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class LiquidityForecastReportingRequestMapperImpl implements LiquidityForecastReportingRequestMapper {
    @Override
    public CreateReviewRequest shareLiquidityForecastReportStage0(ShareLiquidityForecastReportRequest request) {
        return CreateReviewRequest.newBuilder()
                .setContext(request.getContext())
                .setReview(Review.newBuilder()
                        .setDueAt(request.getContext().getDeadline())
                        .addLines(ReviewLine.newBuilder()
                                .putAllAttributes(request.getAnnotationsMap())
                                .build())
                        .putAllMetadata(request.getDetails().getVariablesMap())
                        .build())
                .build();
    }
}
