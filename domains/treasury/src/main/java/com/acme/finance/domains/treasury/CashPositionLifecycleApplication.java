package com.acme.finance.domains.treasury;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.ScoreAuditEventRequest;
import com.acme.audit.v1.ScoreAuditEventResponse;
import com.acme.audit.v1.StreamRetentionPolicyNoticesRequest;
import com.acme.audit.v1.StreamRetentionPolicyNoticesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CreateCustomerRequest;
import com.acme.customers.v1.CreateCustomerResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerServiceGrpc;
import com.acme.customers.v1.SimulateCustomerSegmentRequest;
import com.acme.customers.v1.SimulateCustomerSegmentResponse;
import com.acme.customers.v1.UpdateCustomerRequest;
import com.acme.customers.v1.UpdateCustomerResponse;
import com.acme.documents.v1.DocumentQueryServiceGrpc;
import com.acme.documents.v1.FindDocumentsRequest;
import com.acme.documents.v1.FindDocumentsResponse;
import com.acme.expenses.v1.MileageClaimLifecycleServiceGrpc;
import com.acme.expenses.v1.ScheduleMileageClaimExpiryRequest;
import com.acme.expenses.v1.ScheduleMileageClaimExpiryResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.treasury.v1.ActivateCashPositionRequest;
import com.acme.treasury.v1.ActivateCashPositionResponse;
import com.acme.treasury.v1.BankTransferReportingServiceGrpc;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionRef;
import com.acme.treasury.v1.ExpireCashPositionRequest;
import com.acme.treasury.v1.ExpireCashPositionResponse;
import com.acme.treasury.v1.GetCashPositionLifecycleRequest;
import com.acme.treasury.v1.GetCashPositionLifecycleResponse;
import com.acme.treasury.v1.LintLiquidityForecastsRequest;
import com.acme.treasury.v1.LintLiquidityForecastsResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.LiquidityForecastImportServiceGrpc;
import com.acme.treasury.v1.LiquidityForecastValidationServiceGrpc;
import com.acme.treasury.v1.ListLiquidityForecastImportsRequest;
import com.acme.treasury.v1.ListLiquidityForecastImportsResponse;
import com.acme.treasury.v1.ReinstateCashPositionRequest;
import com.acme.treasury.v1.ReinstateCashPositionResponse;
import com.acme.treasury.v1.RenewCashPositionRequest;
import com.acme.treasury.v1.RenewCashPositionResponse;
import com.acme.treasury.v1.RetryLiquidityForecastImportRequest;
import com.acme.treasury.v1.RetryLiquidityForecastImportResponse;
import com.acme.treasury.v1.ScheduleBankTransferReportRequest;
import com.acme.treasury.v1.ScheduleBankTransferReportResponse;
import com.acme.treasury.v1.ScheduleCashPositionExpiryRequest;
import com.acme.treasury.v1.ScheduleCashPositionExpiryResponse;
import com.acme.treasury.v1.ShareBankTransferReportRequest;
import com.acme.treasury.v1.ShareBankTransferReportResponse;
import com.acme.treasury.v1.SimulateLiquidityForecastRequest;
import com.acme.treasury.v1.SimulateLiquidityForecastResponse;
import com.acme.treasury.v1.SuspendCashPositionRequest;
import com.acme.treasury.v1.SuspendCashPositionResponse;
import com.acme.treasury.v1.TerminateCashPositionRequest;
import com.acme.treasury.v1.TerminateCashPositionResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Moves a cash position between the states of its lifecycle. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class CashPositionLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(CashPositionLifecycleApplication.class);

    private final AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking;

    private final RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking;

    private final ChannelRegistry channels;

    private final CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking;

    private final CustomerServiceGrpc.CustomerServiceBlockingStub customersCustomerServiceBlocking;

    private final CustomerServiceGrpc.CustomerServiceFutureStub customersCustomerServiceFuture;

    private final MileageClaimLifecycleServiceGrpc.MileageClaimLifecycleServiceBlockingStub expensesMileageClaimLifecycleServiceBlocking;

    private final CashPositionLifecycleRequestMapper mapper;

    private final TreasuryRepository repository;

    private final Executor rpcExecutor;

    private final BankTransferReportingServiceGrpc.BankTransferReportingServiceBlockingStub treasuryBankTransferReportingServiceBlocking;

    private final LiquidityForecastImportServiceGrpc.LiquidityForecastImportServiceBlockingStub treasuryLiquidityForecastImportServiceBlocking;

    private final LiquidityForecastImportServiceGrpc.LiquidityForecastImportServiceFutureStub treasuryLiquidityForecastImportServiceFuture;

    private final LiquidityForecastValidationServiceGrpc.LiquidityForecastValidationServiceBlockingStub treasuryLiquidityForecastValidationServiceBlocking;

    public CashPositionLifecycleApplication(
            AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking,
            RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking,
            ChannelRegistry channels,
            CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking,
            CustomerServiceGrpc.CustomerServiceBlockingStub customersCustomerServiceBlocking,
            CustomerServiceGrpc.CustomerServiceFutureStub customersCustomerServiceFuture,
            MileageClaimLifecycleServiceGrpc.MileageClaimLifecycleServiceBlockingStub expensesMileageClaimLifecycleServiceBlocking,
            CashPositionLifecycleRequestMapper mapper,
            TreasuryRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            BankTransferReportingServiceGrpc.BankTransferReportingServiceBlockingStub treasuryBankTransferReportingServiceBlocking,
            LiquidityForecastImportServiceGrpc.LiquidityForecastImportServiceBlockingStub treasuryLiquidityForecastImportServiceBlocking,
            LiquidityForecastImportServiceGrpc.LiquidityForecastImportServiceFutureStub treasuryLiquidityForecastImportServiceFuture,
            LiquidityForecastValidationServiceGrpc.LiquidityForecastValidationServiceBlockingStub treasuryLiquidityForecastValidationServiceBlocking) {
        this.auditAuditEventValidationServiceBlocking = auditAuditEventValidationServiceBlocking;
        this.auditRetentionPolicyNotificationServiceBlocking = auditRetentionPolicyNotificationServiceBlocking;
        this.channels = channels;
        this.customersCustomerSegmentCalculationServiceBlocking = customersCustomerSegmentCalculationServiceBlocking;
        this.customersCustomerServiceBlocking = customersCustomerServiceBlocking;
        this.customersCustomerServiceFuture = customersCustomerServiceFuture;
        this.expensesMileageClaimLifecycleServiceBlocking = expensesMileageClaimLifecycleServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.treasuryBankTransferReportingServiceBlocking = treasuryBankTransferReportingServiceBlocking;
        this.treasuryLiquidityForecastImportServiceBlocking = treasuryLiquidityForecastImportServiceBlocking;
        this.treasuryLiquidityForecastImportServiceFuture = treasuryLiquidityForecastImportServiceFuture;
        this.treasuryLiquidityForecastValidationServiceBlocking = treasuryLiquidityForecastValidationServiceBlocking;
    }

    /**
     * Activates a cash position.
     */
    public ActivateCashPositionResponse activateCashPosition(ActivateCashPositionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivateCashPosition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CashPosition", request.getCashPositionId(), "ActivateCashPosition");

        var outbound0 = ShareBankTransferReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareBankTransferReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .setName(request.getDetails().getNotifyContact().getName())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<ShareBankTransferReportRequest, ShareBankTransferReportResponse>() {
            @Override
            public ShareBankTransferReportResponse apply(ShareBankTransferReportRequest input) {
                return treasuryBankTransferReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareBankTransferReport(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = RetryLiquidityForecastImportRequest.newBuilder()
                .setContext(request.getContext())
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getBankTransfer().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var pending1 = treasuryLiquidityForecastImportServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).retryLiquidityForecastImport(outbound1);
        var reply1 = RpcSupport.await(pending1);

        var outbound2 = StreamRetentionPolicyNoticesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamRetentionPolicyNoticesRequest.Filter.newBuilder()
                        .setRegion(reply0.getBankTransfer().getBillingAddress().getRegion())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getNotifyContact().getEmail())
                        .setName(request.getDetails().getNotifyContact().getName())
                        .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                        .build())
                .build();
        var stream2 = auditRetentionPolicyNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyNotices(outbound2);
        var reply2 = StreamRetentionPolicyNoticesResponse.getDefaultInstance();
        while (stream2.hasNext()) {
            reply2 = stream2.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivateCashPosition", request.getDetails().getReasonCode());
        return ActivateCashPositionResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getLiquidityForecast().getAudit().getCreatedAt())
                                .build())
                        .setCashPositionId(request.getCashPositionId())
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotifyContact().getName())
                                .build())
                        .build())
                .setOutcome(ActivateCashPositionResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Suspends a cash position.
     */
    public SuspendCashPositionResponse suspendCashPosition(SuspendCashPositionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendCashPosition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CashPosition", request.getCashPositionId(), "SuspendCashPosition");

        var outbound0 = RpcSupport.unpack(Any.pack(ListLiquidityForecastImportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListLiquidityForecastImportsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getCashPositionId())
                        .build())
                .build()), ListLiquidityForecastImportsRequest.class);
        var reply0 = treasuryLiquidityForecastImportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listLiquidityForecastImports(outbound0);

        var outbound1 = CreateCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .setName(request.getDetails().getNotifyContact().getName())
                        .build())
                .build();
        var pending1 = customersCustomerServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).createCustomer(outbound1);
        var reply1 = RpcSupport.await(pending1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendCashPosition", request.getDetails().getReasonCode());
        return SuspendCashPositionResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getCustomer().getAudit().getCreatedAt())
                                .build())
                        .setCashPositionId(request.getCashPositionId())
                        .setContact(ContactInfo.newBuilder()
                                .setName(reply1.getCustomer().getName())
                                .build())
                        .build())
                .setOutcome(SuspendCashPositionResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Reinstates a cash position.
     */
    public ReinstateCashPositionResponse reinstateCashPosition(ReinstateCashPositionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReinstateCashPosition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CashPosition", request.getCashPositionId(), "ReinstateCashPosition");

        var outbound0 = ListLiquidityForecastImportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListLiquidityForecastImportsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.treasury.v1.LiquidityForecastImportService/ListLiquidityForecastImports", outbound0);
        var reply0 = treasuryLiquidityForecastImportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listLiquidityForecastImports(command0.payload());
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReinstateCashPosition", request.getDetails().getReasonCode());
        return ReinstateCashPositionResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setCashPositionId(request.getCashPositionId())
                        .build())
                .setOutcome(ReinstateCashPositionResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Terminates a cash position.
     */
    public TerminateCashPositionResponse terminateCashPosition(TerminateCashPositionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TerminateCashPosition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CashPosition", request.getCashPositionId(), "TerminateCashPosition");

        var outbound0 = ScheduleMileageClaimExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScheduleMileageClaimExpiryRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotifyContact().getName())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .build())
                        .build())
                .build();
        var reply0 = expensesMileageClaimLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleMileageClaimExpiry(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TerminateCashPosition", request.getDetails().getReasonCode());
        return TerminateCashPositionResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setCashPositionId(request.getCashPositionId())
                        .build())
                .setOutcome(TerminateCashPositionResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Renews a cash position.
     */
    public RenewCashPositionResponse renewCashPosition(RenewCashPositionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RenewCashPosition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CashPosition", request.getCashPositionId(), "RenewCashPosition");

        var outbound0 = ShareBankTransferReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareBankTransferReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setName(request.getDetails().getNotifyContact().getName())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> treasuryBankTransferReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareBankTransferReport(outbound0), rpcExecutor);

        var outbound1 = ScheduleBankTransferReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> treasuryBankTransferReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleBankTransferReport(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = mapper.renewCashPositionStage2(request, reply0);
        var reply2 = treasuryLiquidityForecastValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintLiquidityForecasts(outbound2);

        var outbound3 = FindDocumentsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindDocumentsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getBankTransfer().getAudit().getUpdatedAt())
                        .putAllLabels(reply2.getLiquidityForecast().getMetadataMap())
                        .setOwnerId(reply2.getResult().getCorrelationId())
                        .setRegion(reply0.getBankTransfer().getBillingAddress().getRegion())
                        .build())
                .build();
        var channel3 = channels.lookup("acme.documents.v1.DocumentQueryService");
        var selected3 = DocumentQueryServiceGrpc.newBlockingStub(channel3)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply3 = selected3.findDocuments(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RenewCashPosition", request.getDetails().getReasonCode());
        return RenewCashPositionResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setCashPositionId(request.getCashPositionId())
                        .build())
                .setOutcome(RenewCashPositionResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply2.getOutcome().getMessage())
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply3.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Expires a cash position.
     */
    public ExpireCashPositionResponse expireCashPosition(ExpireCashPositionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExpireCashPosition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CashPosition", request.getCashPositionId(), "ExpireCashPosition");

        var outbound0 = JsonCodec.roundTrip(LintLiquidityForecastsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintLiquidityForecastsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .build(), LintLiquidityForecastsRequest.class);
        var reply0 = treasuryLiquidityForecastValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintLiquidityForecasts(outbound0);

        var outbound1 = SimulateLiquidityForecastRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SimulateLiquidityForecastRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(reply0.getLiquidityForecast().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getLiquidityForecast().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply0.getLiquidityForecast().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.treasury.v1.LiquidityForecastCalculationService");
        var serviceName1 = "acme.treasury.v1" + "." + "LiquidityForecastCalculationService";
        var methodName1 = "Sim" + "ulateLiquidityForecast";
        var descriptor1 = MethodDescriptor.<SimulateLiquidityForecastRequest, SimulateLiquidityForecastResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(SimulateLiquidityForecastRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(SimulateLiquidityForecastResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExpireCashPosition", request.getDetails().getReasonCode());
        return ExpireCashPositionResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getLiquidityForecast().getAudit().getCreatedAt())
                                .setUpdatedAt(reply1.getLiquidityForecast().getAudit().getUpdatedAt())
                                .build())
                        .setCashPositionId(request.getCashPositionId())
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .setOutcome(ExpireCashPositionResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Returns a cash position.
     */
    public GetCashPositionLifecycleResponse getCashPositionLifecycle(GetCashPositionLifecycleRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetCashPositionLifecycle")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CashPosition", request.getCashPositionId(), "GetCashPositionLifecycle");
        return GetCashPositionLifecycleResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setCashPositionId(request.getCashPositionId())
                        .build())
                .build();
    }

    /**
     * Schedules a cash position.
     */
    public ScheduleCashPositionExpiryResponse scheduleCashPositionExpiry(ScheduleCashPositionExpiryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleCashPositionExpiry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCashPosition(request.getContext().getTenantId(), request.getCashPosition());

        var outbound0 = SimulateCustomerSegmentRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getCashPosition().getAudit().getCreatedAt())
                                .setUpdatedBy(request.getCashPosition().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<SimulateCustomerSegmentRequest, SimulateCustomerSegmentResponse>() {
            @Override
            public SimulateCustomerSegmentResponse apply(SimulateCustomerSegmentRequest input) {
                return customersCustomerSegmentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateCustomerSegment(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = UpdateCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getCashPosition().getAudit().getEtag())
                                .setUpdatedBy(reply0.getCustomerSegment().getAudit().getUpdatedBy())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .setPreferredChannel(request.getCashPosition().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply1 = customersCustomerServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateCustomer(outbound1);

        var outbound2 = JsonCodec.roundTrip(ScoreAuditEventRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(AuditTrailRef.newBuilder()
                                .setDisplayName(reply1.getCustomer().getRelationshipManager().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build(), ScoreAuditEventRequest.class);
        var reply2 = auditAuditEventValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreAuditEvent(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getCashPosition().getCashPositionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleCashPositionExpiry", request.getCashPosition().getCashPositionId());
        return ScheduleCashPositionExpiryResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getCustomerSegment().getAudit().getCreatedBy())
                                .setUpdatedAt(reply1.getCustomer().getAudit().getUpdatedAt())
                                .build())
                        .setCashPositionId(request.getCashPositionId())
                        .setContact(ContactInfo.newBuilder()
                                .setName(reply1.getCustomer().getName())
                                .build())
                        .putAllLabels(request.getCashPosition().getLabelsMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }
}
