package com.acme.finance.domains.treasury;

import com.acme.audit.v1.AuditEventExportServiceGrpc;
import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.ListAuditEventExportsRequest;
import com.acme.audit.v1.ListAuditEventExportsResponse;
import com.acme.audit.v1.ResolveAuditEventsRequest;
import com.acme.audit.v1.ResolveAuditEventsResponse;
import com.acme.audit.v1.SuggestAuditEventsRequest;
import com.acme.audit.v1.SuggestAuditEventsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentBankSettlement;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.documents.v1.ValidateDocumentRequest;
import com.acme.documents.v1.ValidateDocumentResponse;
import com.acme.drivers.v1.DriverRef;
import com.acme.expenses.v1.ExpenseReportQueryServiceGrpc;
import com.acme.expenses.v1.ResolveExpenseReportsRequest;
import com.acme.expenses.v1.ResolveExpenseReportsResponse;
import com.acme.featureflags.v1.FeatureFlagQueryServiceGrpc;
import com.acme.featureflags.v1.SuggestFeatureFlagsRequest;
import com.acme.featureflags.v1.SuggestFeatureFlagsResponse;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ExplainUnderwritingCalculationRequest;
import com.acme.insurance.v1.ExplainUnderwritingCalculationResponse;
import com.acme.insurance.v1.QuoteUnderwritingRequest;
import com.acme.insurance.v1.QuoteUnderwritingResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingCalculationServiceGrpc;
import com.acme.insurance.v1.UnderwritingComment;
import com.acme.maintenance.v1.SimulateWorkOrderRequest;
import com.acme.maintenance.v1.SimulateWorkOrderResponse;
import com.acme.maintenance.v1.WorkOrder;
import com.acme.metrics.v1.BrowseMetricSeriesesRequest;
import com.acme.metrics.v1.BrowseMetricSeriesesResponse;
import com.acme.scheduling.v1.CountShiftsRequest;
import com.acme.scheduling.v1.CountShiftsResponse;
import com.acme.scheduling.v1.ShiftQueryServiceGrpc;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferBankSettlement;
import com.acme.treasury.v1.BankTransferCalculationServiceGrpc;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionBankFunding;
import com.acme.treasury.v1.CashPositionValidationServiceGrpc;
import com.acme.treasury.v1.CheckBankTransferEligibilityRequest;
import com.acme.treasury.v1.CheckBankTransferEligibilityResponse;
import com.acme.treasury.v1.CheckCashPositionEligibilityRequest;
import com.acme.treasury.v1.CheckCashPositionEligibilityResponse;
import com.acme.treasury.v1.EstimateBankTransferRequest;
import com.acme.treasury.v1.EstimateBankTransferResponse;
import com.acme.treasury.v1.ExplainBankTransferDecisionRequest;
import com.acme.treasury.v1.ExplainBankTransferDecisionResponse;
import com.acme.treasury.v1.ExplainCashPositionDecisionRequest;
import com.acme.treasury.v1.ExplainCashPositionDecisionResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeCalculationServiceGrpc;
import com.acme.treasury.v1.FxHedgeLine;
import com.acme.treasury.v1.FxHedgeLineTax;
import com.acme.treasury.v1.GetBankTransferRatesRequest;
import com.acme.treasury.v1.GetBankTransferRatesResponse;
import com.acme.treasury.v1.NormalizeBankTransferRequest;
import com.acme.treasury.v1.NormalizeBankTransferResponse;
import com.acme.treasury.v1.PreviewBankTransferRequest;
import com.acme.treasury.v1.PreviewBankTransferResponse;
import com.acme.treasury.v1.ScoreBankTransferRequest;
import com.acme.treasury.v1.ScoreBankTransferResponse;
import com.acme.treasury.v1.SimulateFxHedgeRequest;
import com.acme.treasury.v1.SimulateFxHedgeResponse;
import com.acme.treasury.v1.ValidateBankTransferRequest;
import com.acme.treasury.v1.ValidateBankTransferResponse;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Checks bank transfers against the rules before anything acts on them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class BankTransferValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(BankTransferValidationApplication.class);

    private final AuditEventExportServiceGrpc.AuditEventExportServiceBlockingStub auditAuditEventExportServiceBlocking;

    private final AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking;

    private final DocumentValidationServiceGrpc.DocumentValidationServiceBlockingStub documentsDocumentValidationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final ExpenseReportQueryServiceGrpc.ExpenseReportQueryServiceBlockingStub expensesExpenseReportQueryServiceBlocking;

    private final FeatureFlagQueryServiceGrpc.FeatureFlagQueryServiceBlockingStub featureflagsFeatureFlagQueryServiceBlocking;

    private final UnderwritingCalculationServiceGrpc.UnderwritingCalculationServiceBlockingStub insuranceUnderwritingCalculationServiceBlocking;

    private final TreasuryRepository repository;

    private final ShiftQueryServiceGrpc.ShiftQueryServiceBlockingStub schedulingShiftQueryServiceBlocking;

    private final BankTransferCalculationServiceGrpc.BankTransferCalculationServiceBlockingStub treasuryBankTransferCalculationServiceBlocking;

    private final CashPositionValidationServiceGrpc.CashPositionValidationServiceBlockingStub treasuryCashPositionValidationServiceBlocking;

    private final FxHedgeCalculationServiceGrpc.FxHedgeCalculationServiceBlockingStub treasuryFxHedgeCalculationServiceBlocking;

    public BankTransferValidationApplication(
            AuditEventExportServiceGrpc.AuditEventExportServiceBlockingStub auditAuditEventExportServiceBlocking,
            AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking,
            DocumentValidationServiceGrpc.DocumentValidationServiceBlockingStub documentsDocumentValidationServiceBlocking,
            ApplicationEventPublisher events,
            ExpenseReportQueryServiceGrpc.ExpenseReportQueryServiceBlockingStub expensesExpenseReportQueryServiceBlocking,
            FeatureFlagQueryServiceGrpc.FeatureFlagQueryServiceBlockingStub featureflagsFeatureFlagQueryServiceBlocking,
            UnderwritingCalculationServiceGrpc.UnderwritingCalculationServiceBlockingStub insuranceUnderwritingCalculationServiceBlocking,
            TreasuryRepository repository,
            ShiftQueryServiceGrpc.ShiftQueryServiceBlockingStub schedulingShiftQueryServiceBlocking,
            BankTransferCalculationServiceGrpc.BankTransferCalculationServiceBlockingStub treasuryBankTransferCalculationServiceBlocking,
            CashPositionValidationServiceGrpc.CashPositionValidationServiceBlockingStub treasuryCashPositionValidationServiceBlocking,
            FxHedgeCalculationServiceGrpc.FxHedgeCalculationServiceBlockingStub treasuryFxHedgeCalculationServiceBlocking) {
        this.auditAuditEventExportServiceBlocking = auditAuditEventExportServiceBlocking;
        this.auditAuditEventQueryServiceBlocking = auditAuditEventQueryServiceBlocking;
        this.documentsDocumentValidationServiceBlocking = documentsDocumentValidationServiceBlocking;
        this.events = events;
        this.expensesExpenseReportQueryServiceBlocking = expensesExpenseReportQueryServiceBlocking;
        this.featureflagsFeatureFlagQueryServiceBlocking = featureflagsFeatureFlagQueryServiceBlocking;
        this.insuranceUnderwritingCalculationServiceBlocking = insuranceUnderwritingCalculationServiceBlocking;
        this.repository = repository;
        this.schedulingShiftQueryServiceBlocking = schedulingShiftQueryServiceBlocking;
        this.treasuryBankTransferCalculationServiceBlocking = treasuryBankTransferCalculationServiceBlocking;
        this.treasuryCashPositionValidationServiceBlocking = treasuryCashPositionValidationServiceBlocking;
        this.treasuryFxHedgeCalculationServiceBlocking = treasuryFxHedgeCalculationServiceBlocking;
    }

    public record ExplainBankTransferDecisionStage0Requested(SimulateWorkOrderRequest request, CompletableFuture<SimulateWorkOrderResponse> completion, io.grpc.Context context) {}

    public record ScoreBankTransferStage1Requested(BrowseMetricSeriesesRequest request, CompletableFuture<BrowseMetricSeriesesResponse> completion, io.grpc.Context context) {}

    /**
     * Validates a bank transfer.
     */
    public ValidateBankTransferResponse validateBankTransfer(ValidateBankTransferRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateBankTransfer")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBankTransfer(request.getContext().getTenantId(), request.getBankTransfer());

        var outbound0 = QuoteUnderwritingRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(QuoteUnderwritingRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getBankTransfer().getBillingAddress().getRegion())
                                .build())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .addComments(UnderwritingComment.newBuilder()
                                .setCreatedAt(request.getBankTransfer().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var reply0 = insuranceUnderwritingCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteUnderwriting(outbound0);

        var outbound1 = CheckCashPositionEligibilityRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getUnderwriting().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        Function<CheckCashPositionEligibilityRequest, CheckCashPositionEligibilityResponse> operation1 = treasuryCashPositionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::checkCashPositionEligibility;
        var reply1 = operation1.apply(outbound1);

        var outbound2 = ListAuditEventExportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAuditEventExportsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply1.getCashPosition().getAudit().getCreatedAt())
                        .putAllLabels(reply1.getCashPosition().getLabelsMap())
                        .setOwnerId(reply1.getResult().getCorrelationId())
                        .setRegion(request.getBankTransfer().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply2 = RpcSupport.retryRead(() -> auditAuditEventExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listAuditEventExports(outbound2));

        var outbound3 = EstimateBankTransferRequest.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getBankTransfer().getAudit().getCreatedAt())
                                .build())
                        .setBank(BankTransferBankSettlement.newBuilder()
                                .setIban(request.getBankTransfer().getBank().getIban())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(request.getBankTransfer().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        RpcPort<EstimateBankTransferRequest, EstimateBankTransferResponse> port3 = RpcPort.adapt(treasuryBankTransferCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::estimateBankTransfer);
        var reply3 = port3.exchange(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getBankTransfer().getBankTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateBankTransfer", request.getBankTransfer().getBankTransferId());
        return ValidateBankTransferResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setBankTransferId(request.getBankTransferId())
                        .setTotal(request.getBankTransfer().getTotal())
                        .build())
                .setOutcome(ValidateBankTransferResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Previews a bank transfer.
     */
    public PreviewBankTransferResponse previewBankTransfer(PreviewBankTransferRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewBankTransfer")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBankTransfer(request.getContext().getTenantId(), request.getBankTransfer());

        var builder0 = ResolveAuditEventsRequest.newBuilder();
        builder0.setContext(request.getContext());
        var outbound0 = builder0.build();
        var reply0 = auditAuditEventQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveAuditEvents(outbound0);

        var outbound1 = ValidateDocumentRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateDocumentRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .build())
                .setDocument(Document.newBuilder()
                        .setBank(DocumentBankSettlement.newBuilder()
                                .setAccountHolder(switch (request.getBankTransfer().getSettlementCase()) {
                                    case BANK -> request.getBankTransfer().getBank().getAccountHolder();
                                    default -> "";
                                })
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getBankTransfer().getBillingAddress().getLinesList())
                                .setLocality(request.getBankTransfer().getBillingAddress().getLocality())
                                .build())
                        .build())
                .build();
        var reply1 = documentsDocumentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateDocument(outbound1);

        var builder2 = ExplainUnderwritingCalculationRequest.newBuilder();
        builder2.setContext(request.getContext());
        builder2.setDetails(ExplainUnderwritingCalculationRequest.Details.newBuilder()
                .setContact(ContactInfo.newBuilder()
                        .setEmail(request.getBankTransfer().getCounterparty().getEmail())
                        .build())
                .build());
        var outbound2 = builder2.build();
        var reply2 = insuranceUnderwritingCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainUnderwritingCalculation(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getBankTransfer().getBankTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewBankTransfer", request.getBankTransfer().getBankTransferId());
        return PreviewBankTransferResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setBankTransferId(request.getBankTransferId())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply1.getDocument().getBillingAddress().getLocality())
                                .build())
                        .setNumber(request.getBankTransfer().getNumber())
                        .build())
                .setOutcome(PreviewBankTransferResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllNextSteps(reply2.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Checks a bank transfer.
     */
    public CheckBankTransferEligibilityResponse checkBankTransferEligibility(CheckBankTransferEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckBankTransferEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBankTransfer(request.getContext().getTenantId(), request.getBankTransfer());

        var outbound0 = ResolveExpenseReportsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        final class Stage0Adapter {
            ResolveExpenseReportsResponse fetch(ResolveExpenseReportsRequest input) {
                return expensesExpenseReportQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveExpenseReports(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = SuggestAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestAuditEventsRequest.Filter.newBuilder()
                        .setRegion(request.getBankTransfer().getBillingAddress().getRegion())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.audit.v1.AuditEventQueryService/SuggestAuditEvents", outbound1);
        var reply1 = auditAuditEventQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestAuditEvents(command1.payload());
        log.atDebug().addKeyValue("auditValue", request.getBankTransfer().getBankTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckBankTransferEligibility", request.getBankTransfer().getBankTransferId());
        return CheckBankTransferEligibilityResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setBankTransferId(request.getBankTransferId())
                        .setNumber(request.getBankTransfer().getNumber())
                        .build())
                .setOutcome(CheckBankTransferEligibilityResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Normalizes a bank transfer.
     */
    public NormalizeBankTransferResponse normalizeBankTransfer(NormalizeBankTransferRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeBankTransfer")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBankTransfer(request.getContext().getTenantId(), request.getBankTransfer());

        var outbound0 = JsonCodec.roundTrip(CountShiftsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountShiftsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getBankTransfer().getIssuedAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getBankTransfer().getCounterparty().getCustomerId())
                        .setRegion(request.getBankTransfer().getBillingAddress().getRegion())
                        .build())
                .build(), CountShiftsRequest.class);
        var reply0 = schedulingShiftQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countShifts(outbound0);

        var outbound1 = ExplainCashPositionDecisionRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getBankTransfer().getAudit().getCreatedAt())
                                .build())
                        .setBank(CashPositionBankFunding.newBuilder()
                                .setAccountHolder(switch (request.getBankTransfer().getSettlementCase()) {
                                    case BANK -> request.getBankTransfer().getBank().getAccountHolder();
                                    default -> "";
                                })
                                .setIban(switch (request.getBankTransfer().getSettlementCase()) {
                                    case BANK -> request.getBankTransfer().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ExplainCashPositionDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .build();
        var reply1 = treasuryCashPositionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainCashPositionDecision(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getBankTransfer().getBankTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeBankTransfer", request.getBankTransfer().getBankTransferId());
        return NormalizeBankTransferResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getCashPosition().getAudit().getCreatedAt())
                                .build())
                        .setBankTransferId(request.getBankTransferId())
                        .setCounterparty(request.getBankTransfer().getCounterparty())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Explains a bank transfer.
     */
    public ExplainBankTransferDecisionResponse explainBankTransferDecision(ExplainBankTransferDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainBankTransferDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBankTransfer(request.getContext().getTenantId(), request.getBankTransfer());

        var outbound0 = SimulateWorkOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setWorkOrder(WorkOrder.newBuilder()
                        .setCustomer(DriverRef.newBuilder()
                                .setDisplayName(request.getBankTransfer().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<SimulateWorkOrderResponse>();
        events.publishEvent(new ExplainBankTransferDecisionStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var builder1 = GetBankTransferRatesRequest.newBuilder();
        builder1.setBankTransferId(request.getBankTransfer().getBankTransferId());
        builder1.setContext(request.getContext());
        var outbound1 = builder1.build();
        var reply1 = treasuryBankTransferCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getBankTransferRates(outbound1);

        List<SimulateFxHedgeResponse> results2 = request.getBankTransfer().getLinesList().stream()
                .map(element -> SimulateFxHedgeRequest.newBuilder()
                .setContext(request.getContext())
                .setFxHedge(FxHedge.newBuilder()
                        .addLines(FxHedgeLine.newBuilder()
                                .setTax(FxHedgeLineTax.newBuilder()
                                        .setAmount(element.getTax().getAmount())
                                        .addAllExemptions(element.getTax().getExemptionsList())
                                        .build())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(treasuryFxHedgeCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateFxHedge(value)))
                .collect(Collectors.toList());
        var reply2 = results2.isEmpty() ? SimulateFxHedgeResponse.getDefaultInstance()
                : results2.getLast();

        var builder3 = SuggestFeatureFlagsRequest.newBuilder();
        builder3.setContext(request.getContext());
        builder3.setFilter(SuggestFeatureFlagsRequest.Filter.newBuilder()
                .setCreatedAfter(reply1.getBankTransfer().getAudit().getUpdatedAt())
                .putAllLabels(reply2.getFxHedge().getMetadataMap())
                .setOwnerId(request.getBankTransfer().getCounterparty().getCustomerId())
                .setRegion(reply1.getBankTransfer().getBillingAddress().getRegion())
                .build());
        var outbound3 = builder3.build();
        var reply3 = featureflagsFeatureFlagQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestFeatureFlags(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getBankTransfer().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainBankTransferDecision", request.getBankTransfer().getNumber());
        return ExplainBankTransferDecisionResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getBankTransfer().getAudit().getUpdatedBy())
                                .build())
                        .setBankTransferId(request.getBankTransferId())
                        .setBillingAddress(reply1.getBankTransfer().getBillingAddress())
                        .setIssuedAt(request.getBankTransfer().getIssuedAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply3.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Scores a bank transfer.
     */
    public ScoreBankTransferResponse scoreBankTransfer(ScoreBankTransferRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreBankTransfer")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBankTransfer(request.getContext().getTenantId(), request.getBankTransfer());

        var outbound0 = GetBankTransferRatesRequest.newBuilder()
                .setBankTransferId(request.getBankTransferId())
                .setContext(request.getContext())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> treasuryBankTransferCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getBankTransferRates(value)).orElseThrow();

        var outbound1 = BrowseMetricSeriesesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseMetricSeriesesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getBankTransfer().getAudit().getCreatedAt())
                        .setOwnerId(request.getBankTransfer().getCounterparty().getCustomerId())
                        .setRegion(reply0.getBankTransfer().getBillingAddress().getRegion())
                        .build())
                .build();
        var completion1 = new CompletableFuture<BrowseMetricSeriesesResponse>();
        events.publishEvent(new ScoreBankTransferStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getBankTransfer().getBankTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreBankTransfer", request.getBankTransfer().getBankTransferId());
        return ScoreBankTransferResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setBankTransferId(request.getBankTransferId())
                        .putAllMetadata(reply0.getBankTransfer().getMetadataMap())
                        .setTotal(request.getBankTransfer().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }
}
