package com.acme.finance.domains.treasury;

import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.StreamAuditTrailExportRequest;
import com.acme.audit.v1.StreamAuditTrailExportResponse;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.CountAccessPoliciesRequest;
import com.acme.authorization.v1.CountAccessPoliciesResponse;
import com.acme.authorization.v1.SummarizeAccessPoliciesRequest;
import com.acme.authorization.v1.SummarizeAccessPoliciesResponse;
import com.acme.billing.v1.BatchCreateBillingAccountsRequest;
import com.acme.billing.v1.BatchCreateBillingAccountsResponse;
import com.acme.billing.v1.BillingAccountServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.QuotePreferenceRequest;
import com.acme.customers.v1.QuotePreferenceResponse;
import com.acme.customers.v1.RecalculateCustomerSegmentsRequest;
import com.acme.customers.v1.RecalculateCustomerSegmentsResponse;
import com.acme.customers.v1.ScoreConsentRecordRequest;
import com.acme.customers.v1.ScoreConsentRecordResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicyLifecycleServiceGrpc;
import com.acme.expenses.v1.ExpensePolicySyncServiceGrpc;
import com.acme.expenses.v1.ScheduleExpensePolicyExpiryRequest;
import com.acme.expenses.v1.ScheduleExpensePolicyExpiryResponse;
import com.acme.expenses.v1.StreamExpensePolicyChangesRequest;
import com.acme.expenses.v1.StreamExpensePolicyChangesResponse;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.CommentLoanApprovalRequest;
import com.acme.lending.v1.CommentLoanApprovalResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApprovalServiceGrpc;
import com.acme.lending.v1.LoanLimit;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferCalculationServiceGrpc;
import com.acme.treasury.v1.BankTransferCardSettlement;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionCardFunding;
import com.acme.treasury.v1.CashPositionLifecycleServiceGrpc;
import com.acme.treasury.v1.CashPositionRef;
import com.acme.treasury.v1.CashPositionValidationServiceGrpc;
import com.acme.treasury.v1.CheckCashPositionEligibilityRequest;
import com.acme.treasury.v1.CheckCashPositionEligibilityResponse;
import com.acme.treasury.v1.CompareLiquidityForecastOptionsRequest;
import com.acme.treasury.v1.CompareLiquidityForecastOptionsResponse;
import com.acme.treasury.v1.ExplainBankTransferCalculationRequest;
import com.acme.treasury.v1.ExplainBankTransferCalculationResponse;
import com.acme.treasury.v1.ExplainFxHedgeDecisionRequest;
import com.acme.treasury.v1.ExplainFxHedgeDecisionResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeCalculationServiceGrpc;
import com.acme.treasury.v1.FxHedgeSyncServiceGrpc;
import com.acme.treasury.v1.FxHedgeValidationServiceGrpc;
import com.acme.treasury.v1.GetLiquidityForecastSyncStateRequest;
import com.acme.treasury.v1.GetLiquidityForecastSyncStateResponse;
import com.acme.treasury.v1.LintCashPositionsRequest;
import com.acme.treasury.v1.LintCashPositionsResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.LiquidityForecastCalculationServiceGrpc;
import com.acme.treasury.v1.LiquidityForecastImportServiceGrpc;
import com.acme.treasury.v1.LiquidityForecastLine;
import com.acme.treasury.v1.ListFxHedgeConflictsRequest;
import com.acme.treasury.v1.ListFxHedgeConflictsResponse;
import com.acme.treasury.v1.ListLiquidityForecastConflictsRequest;
import com.acme.treasury.v1.ListLiquidityForecastConflictsResponse;
import com.acme.treasury.v1.MapLiquidityForecastImportColumnsRequest;
import com.acme.treasury.v1.MapLiquidityForecastImportColumnsResponse;
import com.acme.treasury.v1.PullLiquidityForecastsRequest;
import com.acme.treasury.v1.PullLiquidityForecastsResponse;
import com.acme.treasury.v1.PushLiquidityForecastRequest;
import com.acme.treasury.v1.PushLiquidityForecastResponse;
import com.acme.treasury.v1.RecalculateFxHedgesRequest;
import com.acme.treasury.v1.RecalculateFxHedgesResponse;
import com.acme.treasury.v1.ResolveLiquidityForecastConflictRequest;
import com.acme.treasury.v1.ResolveLiquidityForecastConflictResponse;
import com.acme.treasury.v1.StreamLiquidityForecastChangesRequest;
import com.acme.treasury.v1.StreamLiquidityForecastChangesResponse;
import com.acme.treasury.v1.SyncLiquidityForecastsRequest;
import com.acme.treasury.v1.SyncLiquidityForecastsResponse;
import com.acme.treasury.v1.TerminateCashPositionRequest;
import com.acme.treasury.v1.TerminateCashPositionResponse;
import com.google.protobuf.Any;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class LiquidityForecastSyncRequestMapperImpl implements LiquidityForecastSyncRequestMapper {
    @Override
    public CompareLiquidityForecastOptionsRequest syncLiquidityForecastsStage1(SyncLiquidityForecastsRequest request, CommentLoanApprovalResponse reply0) {
        return CompareLiquidityForecastOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getLoan().getAudit().getCreatedBy())
                                .setEtag(request.getLiquidityForecast().getAudit().getEtag())
                                .build())
                        .build())
                .build();
    }

    @Override
    public LintCashPositionsRequest pushLiquidityForecastStage2(PushLiquidityForecastRequest request, QuotePreferenceResponse reply0) {
        return LintCashPositionsRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getPreference().getAudit().getCreatedAt())
                                .setUpdatedAt(request.getLiquidityForecast().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(LintCashPositionsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getPreference().getOwner().getEmail())
                                .build())
                        .build())
                .build();
    }

    @Override
    public RecalculateCustomerSegmentsRequest getLiquidityForecastSyncStateStage1(GetLiquidityForecastSyncStateRequest request, MapLiquidityForecastImportColumnsResponse reply0) {
        return RecalculateCustomerSegmentsRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getLiquidityForecast().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .setDetails(RecalculateCustomerSegmentsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getLiquidityForecast().getBillingAddress().getCountryCode())
                                .addAllLines(reply0.getLiquidityForecast().getBillingAddress().getLinesList())
                                .setRecipient(reply0.getLiquidityForecast().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
    }
}
