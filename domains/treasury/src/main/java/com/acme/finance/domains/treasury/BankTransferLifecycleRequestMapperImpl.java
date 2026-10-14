package com.acme.finance.domains.treasury;

import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastApprovalServiceGrpc;
import com.acme.budgeting.v1.RejectForecastRequest;
import com.acme.budgeting.v1.RejectForecastResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.BrowseCustomersRequest;
import com.acme.customers.v1.BrowseCustomersResponse;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CountPremiumTotalsRequest;
import com.acme.insurance.v1.CountPremiumTotalsResponse;
import com.acme.localization.v1.BatchCreateTranslationKeysRequest;
import com.acme.localization.v1.BatchCreateTranslationKeysResponse;
import com.acme.localization.v1.TranslationKeyServiceGrpc;
import com.acme.modelregistry.v1.CountRegisteredModelsRequest;
import com.acme.modelregistry.v1.CountRegisteredModelsResponse;
import com.acme.modelregistry.v1.RegisteredModelQueryServiceGrpc;
import com.acme.notifications.v1.CheckNotificationEligibilityRequest;
import com.acme.notifications.v1.CheckNotificationEligibilityResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationValidationServiceGrpc;
import com.acme.payroll.v1.ShareTaxWithholdingReportRequest;
import com.acme.payroll.v1.ShareTaxWithholdingReportResponse;
import com.acme.payroll.v1.TaxWithholdingReportingServiceGrpc;
import com.acme.timekeeping.v1.CountTimesheetsRequest;
import com.acme.timekeeping.v1.CountTimesheetsResponse;
import com.acme.timekeeping.v1.TimesheetQueryServiceGrpc;
import com.acme.treasury.v1.ActivateBankTransferRequest;
import com.acme.treasury.v1.ActivateBankTransferResponse;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferCalculationServiceGrpc;
import com.acme.treasury.v1.BankTransferLine;
import com.acme.treasury.v1.BankTransferReportingServiceGrpc;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionBankFunding;
import com.acme.treasury.v1.CashPositionCalculationServiceGrpc;
import com.acme.treasury.v1.CashPositionQueryServiceGrpc;
import com.acme.treasury.v1.CashPositionRef;
import com.acme.treasury.v1.ExpireBankTransferRequest;
import com.acme.treasury.v1.ExpireBankTransferResponse;
import com.acme.treasury.v1.ExplainBankTransferCalculationRequest;
import com.acme.treasury.v1.ExplainBankTransferCalculationResponse;
import com.acme.treasury.v1.ExplainCashPositionCalculationRequest;
import com.acme.treasury.v1.ExplainCashPositionCalculationResponse;
import com.acme.treasury.v1.FetchBankTransferReportRequest;
import com.acme.treasury.v1.FetchBankTransferReportResponse;
import com.acme.treasury.v1.FxHedgeRef;
import com.acme.treasury.v1.FxHedgeReportingServiceGrpc;
import com.acme.treasury.v1.FxHedgeSyncServiceGrpc;
import com.acme.treasury.v1.GenerateFxHedgeReportRequest;
import com.acme.treasury.v1.GenerateFxHedgeReportResponse;
import com.acme.treasury.v1.GetBankTransferLifecycleRequest;
import com.acme.treasury.v1.GetBankTransferLifecycleResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.LiquidityForecastSyncServiceGrpc;
import com.acme.treasury.v1.ListFxHedgeConflictsRequest;
import com.acme.treasury.v1.ListFxHedgeConflictsResponse;
import com.acme.treasury.v1.ReinstateBankTransferRequest;
import com.acme.treasury.v1.ReinstateBankTransferResponse;
import com.acme.treasury.v1.RenewBankTransferRequest;
import com.acme.treasury.v1.RenewBankTransferResponse;
import com.acme.treasury.v1.ScheduleBankTransferExpiryRequest;
import com.acme.treasury.v1.ScheduleBankTransferExpiryResponse;
import com.acme.treasury.v1.SummarizeCashPositionsRequest;
import com.acme.treasury.v1.SummarizeCashPositionsResponse;
import com.acme.treasury.v1.SuspendBankTransferRequest;
import com.acme.treasury.v1.SuspendBankTransferResponse;
import com.acme.treasury.v1.SyncLiquidityForecastsRequest;
import com.acme.treasury.v1.SyncLiquidityForecastsResponse;
import com.acme.treasury.v1.TerminateBankTransferRequest;
import com.acme.treasury.v1.TerminateBankTransferResponse;
import com.acme.venues.v1.ExplainVenueDecisionRequest;
import com.acme.venues.v1.ExplainVenueDecisionResponse;
import com.acme.venues.v1.LintVenuesRequest;
import com.acme.venues.v1.LintVenuesResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueComponent;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class BankTransferLifecycleRequestMapperImpl implements BankTransferLifecycleRequestMapper {
    @Override
    public CountRegisteredModelsRequest suspendBankTransferStage2(SuspendBankTransferRequest request, ShareTaxWithholdingReportResponse reply0, ExplainBankTransferCalculationResponse reply1) {
        return CountRegisteredModelsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountRegisteredModelsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getTaxWithholding().getAudit().getCreatedAt())
                        .putAllLabels(reply1.getBankTransfer().getMetadataMap())
                        .setOwnerId(reply1.getBankTransfer().getCounterparty().getCustomerId())
                        .setRegion(reply1.getBankTransfer().getBillingAddress().getRegion())
                        .build())
                .build();
    }
}
