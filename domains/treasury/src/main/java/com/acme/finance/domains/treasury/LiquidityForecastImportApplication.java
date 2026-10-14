package com.acme.finance.domains.treasury;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.BrowseCustomersRequest;
import com.acme.customers.v1.BrowseCustomersResponse;
import com.acme.customers.v1.CompareHouseholdOptionsRequest;
import com.acme.customers.v1.CompareHouseholdOptionsResponse;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.ExplainHouseholdDecisionRequest;
import com.acme.customers.v1.ExplainHouseholdDecisionResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.BrowseUsersRequest;
import com.acme.identity.v1.BrowseUsersResponse;
import com.acme.identity.v1.UserQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.learning.v1.Course;
import com.acme.learning.v1.CreateCourseRequest;
import com.acme.learning.v1.CreateCourseResponse;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.ShareFiscalPeriodExportRequest;
import com.acme.ledger.v1.ShareFiscalPeriodExportResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesValidationServiceGrpc;
import com.acme.metrics.v1.ScoreMetricSeriesRequest;
import com.acme.metrics.v1.ScoreMetricSeriesResponse;
import com.acme.payroll.v1.ScheduleTaxWithholdingExpiryRequest;
import com.acme.payroll.v1.ScheduleTaxWithholdingExpiryResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.payroll.v1.TaxWithholdingLifecycleServiceGrpc;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferAttachment;
import com.acme.treasury.v1.BankTransferCalculationServiceGrpc;
import com.acme.treasury.v1.BankTransferQueryServiceGrpc;
import com.acme.treasury.v1.CancelLiquidityForecastImportRequest;
import com.acme.treasury.v1.CancelLiquidityForecastImportResponse;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionCalculationServiceGrpc;
import com.acme.treasury.v1.CashPositionRef;
import com.acme.treasury.v1.CashPositionServiceGrpc;
import com.acme.treasury.v1.CompareBankTransferOptionsRequest;
import com.acme.treasury.v1.CompareBankTransferOptionsResponse;
import com.acme.treasury.v1.CompareCashPositionOptionsRequest;
import com.acme.treasury.v1.CompareCashPositionOptionsResponse;
import com.acme.treasury.v1.CountBankTransfersRequest;
import com.acme.treasury.v1.CountBankTransfersResponse;
import com.acme.treasury.v1.CreateCashPositionRequest;
import com.acme.treasury.v1.CreateCashPositionResponse;
import com.acme.treasury.v1.ExplainCashPositionCalculationRequest;
import com.acme.treasury.v1.ExplainCashPositionCalculationResponse;
import com.acme.treasury.v1.GetLiquidityForecastImportStatusRequest;
import com.acme.treasury.v1.GetLiquidityForecastImportStatusResponse;
import com.acme.treasury.v1.ImportLiquidityForecastsRequest;
import com.acme.treasury.v1.ImportLiquidityForecastsResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.ListLiquidityForecastImportsRequest;
import com.acme.treasury.v1.ListLiquidityForecastImportsResponse;
import com.acme.treasury.v1.MapLiquidityForecastImportColumnsRequest;
import com.acme.treasury.v1.MapLiquidityForecastImportColumnsResponse;
import com.acme.treasury.v1.RecalculateBankTransfersRequest;
import com.acme.treasury.v1.RecalculateBankTransfersResponse;
import com.acme.treasury.v1.RetryLiquidityForecastImportRequest;
import com.acme.treasury.v1.RetryLiquidityForecastImportResponse;
import com.acme.treasury.v1.ValidateLiquidityForecastImportRequest;
import com.acme.treasury.v1.ValidateLiquidityForecastImportResponse;
import com.acme.venues.v1.CountVenuesRequest;
import com.acme.venues.v1.CountVenuesResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Takes liquidity forecasts in bulk from partners and legacy systems. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class LiquidityForecastImportApplication {
    private static final Logger log = LoggerFactory.getLogger(LiquidityForecastImportApplication.class);

    private final ChannelRegistry channels;

    private final CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking;

    private final HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking;

    private final HouseholdValidationServiceGrpc.HouseholdValidationServiceFutureStub customersHouseholdValidationServiceFuture;

    private final ApplicationEventPublisher events;

    private final UserQueryServiceGrpc.UserQueryServiceBlockingStub identityUserQueryServiceBlocking;

    private final FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking;

    private final MetricSeriesValidationServiceGrpc.MetricSeriesValidationServiceBlockingStub metricsMetricSeriesValidationServiceBlocking;

    private final TaxWithholdingLifecycleServiceGrpc.TaxWithholdingLifecycleServiceBlockingStub payrollTaxWithholdingLifecycleServiceBlocking;

    private final TreasuryRepository repository;

    private final Executor rpcExecutor;

    private final BankTransferCalculationServiceGrpc.BankTransferCalculationServiceBlockingStub treasuryBankTransferCalculationServiceBlocking;

    private final BankTransferQueryServiceGrpc.BankTransferQueryServiceBlockingStub treasuryBankTransferQueryServiceBlocking;

    private final CashPositionCalculationServiceGrpc.CashPositionCalculationServiceBlockingStub treasuryCashPositionCalculationServiceBlocking;

    private final CashPositionServiceGrpc.CashPositionServiceBlockingStub treasuryCashPositionServiceBlocking;

    public LiquidityForecastImportApplication(
            ChannelRegistry channels,
            CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking,
            HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking,
            HouseholdValidationServiceGrpc.HouseholdValidationServiceFutureStub customersHouseholdValidationServiceFuture,
            ApplicationEventPublisher events,
            UserQueryServiceGrpc.UserQueryServiceBlockingStub identityUserQueryServiceBlocking,
            FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking,
            MetricSeriesValidationServiceGrpc.MetricSeriesValidationServiceBlockingStub metricsMetricSeriesValidationServiceBlocking,
            TaxWithholdingLifecycleServiceGrpc.TaxWithholdingLifecycleServiceBlockingStub payrollTaxWithholdingLifecycleServiceBlocking,
            TreasuryRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            BankTransferCalculationServiceGrpc.BankTransferCalculationServiceBlockingStub treasuryBankTransferCalculationServiceBlocking,
            BankTransferQueryServiceGrpc.BankTransferQueryServiceBlockingStub treasuryBankTransferQueryServiceBlocking,
            CashPositionCalculationServiceGrpc.CashPositionCalculationServiceBlockingStub treasuryCashPositionCalculationServiceBlocking,
            CashPositionServiceGrpc.CashPositionServiceBlockingStub treasuryCashPositionServiceBlocking) {
        this.channels = channels;
        this.customersCustomerQueryServiceBlocking = customersCustomerQueryServiceBlocking;
        this.customersHouseholdCalculationServiceBlocking = customersHouseholdCalculationServiceBlocking;
        this.customersHouseholdValidationServiceFuture = customersHouseholdValidationServiceFuture;
        this.events = events;
        this.identityUserQueryServiceBlocking = identityUserQueryServiceBlocking;
        this.ledgerFiscalPeriodExportServiceBlocking = ledgerFiscalPeriodExportServiceBlocking;
        this.metricsMetricSeriesValidationServiceBlocking = metricsMetricSeriesValidationServiceBlocking;
        this.payrollTaxWithholdingLifecycleServiceBlocking = payrollTaxWithholdingLifecycleServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.treasuryBankTransferCalculationServiceBlocking = treasuryBankTransferCalculationServiceBlocking;
        this.treasuryBankTransferQueryServiceBlocking = treasuryBankTransferQueryServiceBlocking;
        this.treasuryCashPositionCalculationServiceBlocking = treasuryCashPositionCalculationServiceBlocking;
        this.treasuryCashPositionServiceBlocking = treasuryCashPositionServiceBlocking;
    }

    public record RetryLiquidityForecastImportStage0Requested(CountVenuesRequest request, CompletableFuture<CountVenuesResponse> completion, io.grpc.Context context) {}

    /**
     * Takes a stream of liquidity forecasts and answers once at the end.
     */
    public ImportLiquidityForecastsResponse importLiquidityForecasts(ImportLiquidityForecastsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ImportLiquidityForecasts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        if (request.hasRecord()) {
            repository.saveLiquidityForecast(request.getContext().getTenantId(), request.getRecord());
        }

        var outbound0 = CompareCashPositionOptionsRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(switch (request.getChunkCase()) {
                                    case RECORD -> request.getRecord().getAudit().getUpdatedBy();
                                    default -> "";
                                })
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(switch (request.getChunkCase()) {
                                    case HEADER -> request.getHeader().getSubmittedBy().getPhone();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CompareCashPositionOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(switch (request.getChunkCase()) {
                                    case RECORD -> request.getRecord().getBillingAddress().getRecipient();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = treasuryCashPositionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareCashPositionOptions(outbound0);

        var outbound1 = CompareHouseholdOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(request.getRecord().getAudit().getCreatedAt())
                                                .setEtag(reply0.getCashPosition().getAudit().getEtag())
                                                .setUpdatedBy(reply0.getCashPosition().getAudit().getUpdatedBy())
                                                .build())
                                        .setLocation(PostalAddress.newBuilder()
                                                .setRecipient(switch (request.getChunkCase()) {
                                                    case RECORD -> request.getRecord().getBillingAddress().getRecipient();
                                                    default -> "";
                                                })
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply1 = customersHouseholdCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareHouseholdOptions(outbound1);

        var outbound2 = RecalculateBankTransfersRequest.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getCashPosition().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply0.getCashPosition().getContact().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RecalculateBankTransfersRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getHousehold().getLocation().getCountryCode())
                                .build())
                        .build())
                .build();
        var reply2 = treasuryBankTransferCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateBankTransfers(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getHeader().getSource()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ImportLiquidityForecasts", request.getHeader().getSource());
        return ImportLiquidityForecastsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a liquidity forecast.
     */
    public GetLiquidityForecastImportStatusResponse getLiquidityForecastImportStatus(GetLiquidityForecastImportStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetLiquidityForecastImportStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LiquidityForecast", request.getLiquidityForecastId(), "GetLiquidityForecastImportStatus");

        var outbound0 = (ExplainCashPositionCalculationRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ExplainCashPositionCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build()).toBuilder().build();
        var reply0 = treasuryCashPositionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainCashPositionCalculation(outbound0);
        return GetLiquidityForecastImportStatusResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getCashPosition().getAudit().getCreatedBy())
                                .build())
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .build())
                .build();
    }

    /**
     * Cancels a liquidity forecast.
     */
    public CancelLiquidityForecastImportResponse cancelLiquidityForecastImport(CancelLiquidityForecastImportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelLiquidityForecastImport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LiquidityForecast", request.getLiquidityForecastId(), "CancelLiquidityForecastImport");

        var outbound0 = ScheduleTaxWithholdingExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScheduleTaxWithholdingExpiryRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .build())
                        .build())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> payrollTaxWithholdingLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleTaxWithholdingExpiry(value)).orElseThrow();

        var outbound1 = ExplainHouseholdDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                                .build())
                                        .build())
                                .setManager(UserRef.newBuilder()
                                        .setDisplayName(reply0.getTaxWithholding().getOwner().getDisplayName())
                                        .setUserId(reply0.getTaxWithholding().getOwner().getUserId())
                                        .build())
                                .build())
                        .build())
                .build();
        var pending1 = customersHouseholdValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).explainHouseholdDecision(outbound1);
        var reply1 = RpcSupport.await(pending1);

        var outbound2 = CountBankTransfersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountBankTransfersRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getTaxWithholding().getAudit().getUpdatedAt())
                        .putAllLabels(reply0.getTaxWithholding().getParametersMap())
                        .setOwnerId(reply1.getHousehold().getParentId())
                        .setRegion(reply1.getHousehold().getLocation().getRegion())
                        .build())
                .build();
        var reply2 = treasuryBankTransferQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countBankTransfers(outbound2);

        var outbound3 = CreateCourseRequest.newBuilder()
                .setContext(request.getContext())
                .setCourse(Course.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setRecipient(reply1.getHousehold().getLocation().getRecipient())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getHousehold().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var channel3 = channels.lookup("acme.learning.v1.CourseService");
        var serviceName3 = "acme.learning.v1" + "." + "CourseService";
        var methodName3 = "Cre" + "ateCourse";
        var descriptor3 = MethodDescriptor.<CreateCourseRequest, CreateCourseResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName3, methodName3))
                .setRequestMarshaller(ProtoUtils.marshaller(CreateCourseRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(CreateCourseResponse.getDefaultInstance()))
                .build();
        var reply3 = ClientCalls.blockingUnaryCall(channel3, descriptor3,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelLiquidityForecastImport", request.getDetails().getReasonCode());
        return CancelLiquidityForecastImportResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getTaxWithholding().getAudit().getEtag())
                                .setUpdatedBy(reply1.getHousehold().getAudit().getUpdatedBy())
                                .build())
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .build())
                .setOutcome(CancelLiquidityForecastImportResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply3.getResult().getMessage())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Validates a liquidity forecast.
     */
    public ValidateLiquidityForecastImportResponse validateLiquidityForecastImport(ValidateLiquidityForecastImportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateLiquidityForecastImport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLiquidityForecast(request.getContext().getTenantId(), request.getLiquidityForecast());

        var outbound0 = ScoreMetricSeriesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScoreMetricSeriesRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getLiquidityForecast().getBillingAddress().getLinesList())
                                .setPostalCode(request.getLiquidityForecast().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .setMetricSeries(MetricSeries.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setRegion(request.getLiquidityForecast().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        var reply0 = metricsMetricSeriesValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreMetricSeries(outbound0);

        var outbound1 = ShareFiscalPeriodExportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareFiscalPeriodExportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(reply0.getMetricSeries().getOwner().getEmail())
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        final class Stage1Adapter {
            ShareFiscalPeriodExportResponse fetch(ShareFiscalPeriodExportRequest input) {
                return ledgerFiscalPeriodExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareFiscalPeriodExport(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);

        var futures2 = request.getLiquidityForecast().getAttachmentsList().stream()
                .map(element -> CompareBankTransferOptionsRequest.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .addAttachments(BankTransferAttachment.newBuilder()
                                .setContentType(element.getContentType())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> treasuryBankTransferCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareBankTransferOptions(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures2.toArray(CompletableFuture[]::new)).join();
        var results2 = futures2.stream().map(CompletableFuture::join).toList();
        var reply2 = results2.isEmpty() ? CompareBankTransferOptionsResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getLiquidityForecast().getLiquidityForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateLiquidityForecastImport", request.getLiquidityForecast().getLiquidityForecastId());
        return ValidateLiquidityForecastImportResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply1.getFiscalPeriod().getLocation().getPostalCode())
                                .build())
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setCurrencyCode(reply0.getBreakdown().getCurrencyCode())
                                .build())
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .setNotes(request.getLiquidityForecast().getNotes())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Lists liquidity forecasts a page at a time.
     */
    public ListLiquidityForecastImportsResponse listLiquidityForecastImports(ListLiquidityForecastImportsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListLiquidityForecastImports")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LiquidityForecast", request.getContext().getTraceId(), "ListLiquidityForecastImports");

        var outbound0 = (CreateCashPositionRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .build())
                .setContext(request.getContext())
                .build()).toBuilder().build();
        var reply0 = treasuryCashPositionServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createCashPosition(outbound0);

        var outbound1 = BrowseUsersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseUsersRequest.Filter.newBuilder()
                        .setQuery(request.getFilter().getQuery())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> identityUserQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseUsers(value));
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListLiquidityForecastImports", request.getPage().getPageToken());
        return ListLiquidityForecastImportsResponse.newBuilder()
                .addLiquidityForecasts(LiquidityForecast.newBuilder()
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setCashPositionId(reply0.getCashPosition().getCashPositionId())
                                .build())
                        .setNumber(reply0.getCashPosition().getHolder().getNumber())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Retrys a liquidity forecast.
     */
    public RetryLiquidityForecastImportResponse retryLiquidityForecastImport(RetryLiquidityForecastImportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryLiquidityForecastImport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLiquidityForecast(request.getContext().getTenantId(), request.getLiquidityForecast());

        var outbound0 = CountVenuesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountVenuesRequest.Filter.newBuilder()
                        .setRegion(request.getLiquidityForecast().getBillingAddress().getRegion())
                        .build())
                .build();
        var completion0 = new CompletableFuture<CountVenuesResponse>();
        events.publishEvent(new RetryLiquidityForecastImportStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getLiquidityForecast().getLiquidityForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryLiquidityForecastImport", request.getLiquidityForecast().getLiquidityForecastId());
        return RetryLiquidityForecastImportResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .putAllMetadata(request.getLiquidityForecast().getMetadataMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Maps a liquidity forecast.
     */
    public MapLiquidityForecastImportColumnsResponse mapLiquidityForecastImportColumns(MapLiquidityForecastImportColumnsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "MapLiquidityForecastImportColumns")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLiquidityForecast(request.getContext().getTenantId(), request.getLiquidityForecast());

        var outbound0 = RpcSupport.unpack(Any.pack(BrowseCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseCustomersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getLiquidityForecast().getAudit().getUpdatedAt())
                        .setOwnerId(request.getLiquidityForecast().getCounterparty().getCashPositionId())
                        .setRegion(request.getLiquidityForecast().getBillingAddress().getRegion())
                        .build())
                .build()), BrowseCustomersRequest.class);
        var reply0 = customersCustomerQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseCustomers(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getLiquidityForecast().getLiquidityForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "MapLiquidityForecastImportColumns", request.getLiquidityForecast().getLiquidityForecastId());
        return MapLiquidityForecastImportColumnsResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setBillingAddress(request.getLiquidityForecast().getBillingAddress())
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .build())
                .setOutcome(MapLiquidityForecastImportColumnsResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }
}
