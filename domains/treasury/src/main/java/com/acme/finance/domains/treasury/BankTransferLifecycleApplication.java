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
import org.springframework.stereotype.Service;

/**
 * Moves a bank transfer between the states of its lifecycle. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class BankTransferLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(BankTransferLifecycleApplication.class);

    private final ForecastApprovalServiceGrpc.ForecastApprovalServiceBlockingStub budgetingForecastApprovalServiceBlocking;

    private final ChannelRegistry channels;

    private final CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking;

    private final TranslationKeyServiceGrpc.TranslationKeyServiceStub localizationTranslationKeyServiceAsync;

    private final BankTransferLifecycleRequestMapper mapper;

    private final RegisteredModelQueryServiceGrpc.RegisteredModelQueryServiceBlockingStub modelregistryRegisteredModelQueryServiceBlocking;

    private final NotificationValidationServiceGrpc.NotificationValidationServiceBlockingStub notificationsNotificationValidationServiceBlocking;

    private final TaxWithholdingReportingServiceGrpc.TaxWithholdingReportingServiceBlockingStub payrollTaxWithholdingReportingServiceBlocking;

    private final TreasuryRepository repository;

    private final Executor rpcExecutor;

    private final TimesheetQueryServiceGrpc.TimesheetQueryServiceBlockingStub timekeepingTimesheetQueryServiceBlocking;

    private final BankTransferCalculationServiceGrpc.BankTransferCalculationServiceBlockingStub treasuryBankTransferCalculationServiceBlocking;

    private final BankTransferReportingServiceGrpc.BankTransferReportingServiceBlockingStub treasuryBankTransferReportingServiceBlocking;

    private final CashPositionCalculationServiceGrpc.CashPositionCalculationServiceBlockingStub treasuryCashPositionCalculationServiceBlocking;

    private final CashPositionQueryServiceGrpc.CashPositionQueryServiceBlockingStub treasuryCashPositionQueryServiceBlocking;

    private final FxHedgeReportingServiceGrpc.FxHedgeReportingServiceBlockingStub treasuryFxHedgeReportingServiceBlocking;

    private final FxHedgeSyncServiceGrpc.FxHedgeSyncServiceBlockingStub treasuryFxHedgeSyncServiceBlocking;

    private final LiquidityForecastSyncServiceGrpc.LiquidityForecastSyncServiceBlockingStub treasuryLiquidityForecastSyncServiceBlocking;

    private final VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking;

    public BankTransferLifecycleApplication(
            ForecastApprovalServiceGrpc.ForecastApprovalServiceBlockingStub budgetingForecastApprovalServiceBlocking,
            ChannelRegistry channels,
            CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking,
            TranslationKeyServiceGrpc.TranslationKeyServiceStub localizationTranslationKeyServiceAsync,
            BankTransferLifecycleRequestMapper mapper,
            RegisteredModelQueryServiceGrpc.RegisteredModelQueryServiceBlockingStub modelregistryRegisteredModelQueryServiceBlocking,
            NotificationValidationServiceGrpc.NotificationValidationServiceBlockingStub notificationsNotificationValidationServiceBlocking,
            TaxWithholdingReportingServiceGrpc.TaxWithholdingReportingServiceBlockingStub payrollTaxWithholdingReportingServiceBlocking,
            TreasuryRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            TimesheetQueryServiceGrpc.TimesheetQueryServiceBlockingStub timekeepingTimesheetQueryServiceBlocking,
            BankTransferCalculationServiceGrpc.BankTransferCalculationServiceBlockingStub treasuryBankTransferCalculationServiceBlocking,
            BankTransferReportingServiceGrpc.BankTransferReportingServiceBlockingStub treasuryBankTransferReportingServiceBlocking,
            CashPositionCalculationServiceGrpc.CashPositionCalculationServiceBlockingStub treasuryCashPositionCalculationServiceBlocking,
            CashPositionQueryServiceGrpc.CashPositionQueryServiceBlockingStub treasuryCashPositionQueryServiceBlocking,
            FxHedgeReportingServiceGrpc.FxHedgeReportingServiceBlockingStub treasuryFxHedgeReportingServiceBlocking,
            FxHedgeSyncServiceGrpc.FxHedgeSyncServiceBlockingStub treasuryFxHedgeSyncServiceBlocking,
            LiquidityForecastSyncServiceGrpc.LiquidityForecastSyncServiceBlockingStub treasuryLiquidityForecastSyncServiceBlocking,
            VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking) {
        this.budgetingForecastApprovalServiceBlocking = budgetingForecastApprovalServiceBlocking;
        this.channels = channels;
        this.customersCustomerQueryServiceBlocking = customersCustomerQueryServiceBlocking;
        this.localizationTranslationKeyServiceAsync = localizationTranslationKeyServiceAsync;
        this.mapper = mapper;
        this.modelregistryRegisteredModelQueryServiceBlocking = modelregistryRegisteredModelQueryServiceBlocking;
        this.notificationsNotificationValidationServiceBlocking = notificationsNotificationValidationServiceBlocking;
        this.payrollTaxWithholdingReportingServiceBlocking = payrollTaxWithholdingReportingServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.timekeepingTimesheetQueryServiceBlocking = timekeepingTimesheetQueryServiceBlocking;
        this.treasuryBankTransferCalculationServiceBlocking = treasuryBankTransferCalculationServiceBlocking;
        this.treasuryBankTransferReportingServiceBlocking = treasuryBankTransferReportingServiceBlocking;
        this.treasuryCashPositionCalculationServiceBlocking = treasuryCashPositionCalculationServiceBlocking;
        this.treasuryCashPositionQueryServiceBlocking = treasuryCashPositionQueryServiceBlocking;
        this.treasuryFxHedgeReportingServiceBlocking = treasuryFxHedgeReportingServiceBlocking;
        this.treasuryFxHedgeSyncServiceBlocking = treasuryFxHedgeSyncServiceBlocking;
        this.treasuryLiquidityForecastSyncServiceBlocking = treasuryLiquidityForecastSyncServiceBlocking;
        this.venuesVenueValidationServiceBlocking = venuesVenueValidationServiceBlocking;
    }

    /**
     * Activates a bank transfer.
     */
    public ActivateBankTransferResponse activateBankTransfer(ActivateBankTransferRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivateBankTransfer")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankTransfer", request.getBankTransferId(), "ActivateBankTransfer");

        var outbound0 = CountTimesheetsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountTimesheetsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> timekeepingTimesheetQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countTimesheets(outbound0), rpcExecutor);

        var outbound1 = SyncLiquidityForecastsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SyncLiquidityForecastsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> treasuryLiquidityForecastSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).syncLiquidityForecasts(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivateBankTransfer", request.getDetails().getReasonCode());
        return ActivateBankTransferResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setBankTransferId(request.getBankTransferId())
                        .setIssuedAt(reply1.getLiquidityForecast().getIssuedAt())
                        .build())
                .setOutcome(ActivateBankTransferResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Suspends a bank transfer.
     */
    public SuspendBankTransferResponse suspendBankTransfer(SuspendBankTransferRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendBankTransfer")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankTransfer", request.getBankTransferId(), "SuspendBankTransfer");

        var outbound0 = ShareTaxWithholdingReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareTaxWithholdingReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .setName(request.getDetails().getNotifyContact().getName())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            ShareTaxWithholdingReportResponse fetch(ShareTaxWithholdingReportRequest input) {
                return payrollTaxWithholdingReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareTaxWithholdingReport(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = ExplainBankTransferCalculationRequest.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getTaxWithholding().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .addLines(BankTransferLine.newBuilder()
                                .setDescription(reply0.getTaxWithholding().getDescription())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        Function<ExplainBankTransferCalculationRequest, ExplainBankTransferCalculationResponse> operation1 = treasuryBankTransferCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::explainBankTransferCalculation;
        var reply1 = operation1.apply(outbound1);

        var outbound2 = mapper.suspendBankTransferStage2(request, reply0, reply1);
        var reply2 = modelregistryRegisteredModelQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countRegisteredModels(outbound2);

        var outbound3 = BeanCopies.copy(BrowseCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseCustomersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(reply1.getResult().getDetailsMap())
                        .setOwnerId(reply1.getBankTransfer().getCounterparty().getCustomerId())
                        .setRegion(reply1.getBankTransfer().getBillingAddress().getRegion())
                        .build())
                .build(), BrowseCustomersRequest.class);
        var reply3 = customersCustomerQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseCustomers(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendBankTransfer", request.getDetails().getReasonCode());
        return SuspendBankTransferResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setBankTransferId(request.getBankTransferId())
                        .build())
                .setOutcome(SuspendBankTransferResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply3.getResult().getDetailsMap())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Reinstates a bank transfer.
     */
    public ReinstateBankTransferResponse reinstateBankTransfer(ReinstateBankTransferRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReinstateBankTransfer")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankTransfer", request.getBankTransferId(), "ReinstateBankTransfer");

        var outbound0 = LintVenuesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintVenuesRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotifyContact().getName())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> venuesVenueValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintVenues(outbound0), rpcExecutor);

        var outbound1 = CheckNotificationEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckNotificationEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setName(request.getDetails().getNotifyContact().getName())
                                .build())
                        .build())
                .setNotification(Notification.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> notificationsNotificationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkNotificationEligibility(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReinstateBankTransfer", request.getDetails().getReasonCode());
        return ReinstateBankTransferResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getVenue().getAudit().getCreatedAt())
                                .build())
                        .setBankTransferId(request.getBankTransferId())
                        .addLines(BankTransferLine.newBuilder()
                                .setQuantity(reply0.getVenue().getComponentsOrDefault("primary", VenueComponent.getDefaultInstance()).getQuantity())
                                .build())
                        .build())
                .setOutcome(ReinstateBankTransferResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Terminates a bank transfer.
     */
    public TerminateBankTransferResponse terminateBankTransfer(TerminateBankTransferRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TerminateBankTransfer")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankTransfer", request.getBankTransferId(), "TerminateBankTransfer");

        var outbound0 = ExplainVenueDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainVenueDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .build())
                        .build())
                .setVenue(Venue.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> venuesVenueValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainVenueDecision(outbound0), rpcExecutor);

        var outbound1 = FetchBankTransferReportRequest.newBuilder()
                .setBankTransferId(request.getBankTransferId())
                .setContext(request.getContext())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> treasuryBankTransferReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).fetchBankTransferReport(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = CountPremiumTotalsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountPremiumTotalsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getVenue().getAudit().getCreatedAt())
                        .setOwnerId(reply0.getVenue().getComponentsOrDefault("primary", VenueComponent.getDefaultInstance()).getComponentId())
                        .setRegion(reply1.getBankTransfer().getBillingAddress().getRegion())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.insurance.v1.PremiumReportingService");
        var serviceName2 = "acme.insurance.v1" + "." + "PremiumReportingService";
        var methodName2 = "Cou" + "ntPremiumTotals";
        var descriptor2 = MethodDescriptor.<CountPremiumTotalsRequest, CountPremiumTotalsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(CountPremiumTotalsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(CountPremiumTotalsResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);

        var outbound3 = RejectForecastRequest.newBuilder()
                .setContext(request.getContext())
                .setForecast(Forecast.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply1.getBankTransfer().getBillingAddress().getLocality())
                                .setRegion(reply1.getBankTransfer().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        final class Stage3Adapter {
            RejectForecastResponse fetch(RejectForecastRequest input) {
                return budgetingForecastApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectForecast(input);
            }
        }
        var reply3 = new Stage3Adapter().fetch(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TerminateBankTransfer", request.getDetails().getReasonCode());
        return TerminateBankTransferResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setBankTransferId(request.getBankTransferId())
                        .setBillingAddress(reply1.getBankTransfer().getBillingAddress())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply0.getVenue().getOwner().getDisplayName())
                                .build())
                        .putAllMetadata(reply3.getForecast().getMetadataMap())
                        .build())
                .setOutcome(TerminateBankTransferResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Renews a bank transfer.
     */
    public RenewBankTransferResponse renewBankTransfer(RenewBankTransferRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RenewBankTransfer")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankTransfer", request.getBankTransferId(), "RenewBankTransfer");

        var outbound0 = ListFxHedgeConflictsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListFxHedgeConflictsRequest.Filter.newBuilder()
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        RpcPort<ListFxHedgeConflictsRequest, ListFxHedgeConflictsResponse> port0 = RpcPort.adapt(treasuryFxHedgeSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::listFxHedgeConflicts);
        var reply0 = port0.exchange(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RenewBankTransfer", request.getDetails().getReasonCode());
        return RenewBankTransferResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setBankTransferId(request.getBankTransferId())
                        .build())
                .setOutcome(RenewBankTransferResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Expires a bank transfer.
     */
    public ExpireBankTransferResponse expireBankTransfer(ExpireBankTransferRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExpireBankTransfer")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankTransfer", request.getBankTransferId(), "ExpireBankTransfer");

        var outbound0 = BatchCreateTranslationKeysRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateTranslationKeysRequest.StreamHeader.newBuilder()
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setName(request.getDetails().getNotifyContact().getName())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var collector0 = new StreamCollector<>(BatchCreateTranslationKeysResponse.getDefaultInstance());
        var input0 = localizationTranslationKeyServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateTranslationKeys(collector0);
        input0.onNext(outbound0);
        input0.onCompleted();
        var reply0 = collector0.await();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExpireBankTransfer", request.getDetails().getReasonCode());
        return ExpireBankTransferResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setBankTransferId(request.getBankTransferId())
                        .build())
                .setOutcome(ExpireBankTransferResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Returns a bank transfer.
     */
    public GetBankTransferLifecycleResponse getBankTransferLifecycle(GetBankTransferLifecycleRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetBankTransferLifecycle")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankTransfer", request.getBankTransferId(), "GetBankTransferLifecycle");
        return GetBankTransferLifecycleResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setBankTransferId(request.getBankTransferId())
                        .build())
                .build();
    }

    /**
     * Schedules a bank transfer.
     */
    public ScheduleBankTransferExpiryResponse scheduleBankTransferExpiry(ScheduleBankTransferExpiryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleBankTransferExpiry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBankTransfer(request.getContext().getTenantId(), request.getBankTransfer());

        var outbound0 = ExplainCashPositionCalculationRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setBank(CashPositionBankFunding.newBuilder()
                                .setAccountHolder(request.getBankTransfer().getBank().getAccountHolder())
                                .setIban(request.getBankTransfer().getBank().getIban())
                                .build())
                        .setHolder(FxHedgeRef.newBuilder()
                                .setDisplayName(request.getBankTransfer().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = treasuryCashPositionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainCashPositionCalculation(outbound0);

        var outbound1 = GenerateFxHedgeReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setFxHedgeId(reply0.getCashPosition().getHolder().getFxHedgeId())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> treasuryFxHedgeReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).generateFxHedgeReport(value));

        var outbound2 = SummarizeCashPositionsRequest.newBuilder()
                .setCashPositionId(reply0.getCashPosition().getCashPositionId())
                .setContext(request.getContext())
                .setDetails(SummarizeCashPositionsRequest.Details.newBuilder()
                        .setPeriod(reply1.getPeriod())
                        .build())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(treasuryCashPositionQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeCashPositions(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getBankTransfer().getBankTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleBankTransferExpiry", request.getBankTransfer().getBankTransferId());
        return ScheduleBankTransferExpiryResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getCashPosition().getAudit().getCreatedBy())
                                .setUpdatedAt(reply0.getCashPosition().getAudit().getUpdatedAt())
                                .build())
                        .setBankTransferId(request.getBankTransferId())
                        .setNotes(request.getBankTransfer().getNotes())
                        .build())
                .setOutcome(ScheduleBankTransferExpiryResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }
}
