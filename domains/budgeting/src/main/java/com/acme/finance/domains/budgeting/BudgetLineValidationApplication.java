package com.acme.finance.domains.budgeting;

import com.acme.admissions.v1.Admission;
import com.acme.admissions.v1.AdmissionCalculationServiceGrpc;
import com.acme.admissions.v1.CalculateAdmissionRequest;
import com.acme.admissions.v1.CalculateAdmissionResponse;
import com.acme.admissions.v1.GateScanRef;
import com.acme.audiences.v1.AudienceServiceGrpc;
import com.acme.audiences.v1.ListAudiencesRequest;
import com.acme.audiences.v1.ListAudiencesResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailCardSettlement;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.AuditTrailSyncServiceGrpc;
import com.acme.audit.v1.LintAuditEventsRequest;
import com.acme.audit.v1.LintAuditEventsResponse;
import com.acme.audit.v1.ReconcileAuditTrailsRequest;
import com.acme.audit.v1.ReconcileAuditTrailsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyDiscountAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.RetentionPolicyValidationServiceGrpc;
import com.acme.audit.v1.ScoreRetentionPolicyRequest;
import com.acme.audit.v1.ScoreRetentionPolicyResponse;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.ValidateAuditEventRequest;
import com.acme.audit.v1.ValidateAuditEventResponse;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetLineLine;
import com.acme.budgeting.v1.BudgetRef;
import com.acme.budgeting.v1.CalculateBudgetLineRequest;
import com.acme.budgeting.v1.CalculateBudgetLineResponse;
import com.acme.budgeting.v1.CalculateForecastRequest;
import com.acme.budgeting.v1.CalculateForecastResponse;
import com.acme.budgeting.v1.CheckBudgetLineEligibilityRequest;
import com.acme.budgeting.v1.CheckBudgetLineEligibilityResponse;
import com.acme.budgeting.v1.ExplainBudgetLineDecisionRequest;
import com.acme.budgeting.v1.ExplainBudgetLineDecisionResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.NormalizeBudgetLineRequest;
import com.acme.budgeting.v1.NormalizeBudgetLineResponse;
import com.acme.budgeting.v1.PreviewBudgetLineRequest;
import com.acme.budgeting.v1.PreviewBudgetLineResponse;
import com.acme.budgeting.v1.ScoreBudgetLineRequest;
import com.acme.budgeting.v1.ScoreBudgetLineResponse;
import com.acme.budgeting.v1.SimulateBudgetLineRequest;
import com.acme.budgeting.v1.SimulateBudgetLineResponse;
import com.acme.budgeting.v1.ValidateBudgetLineRequest;
import com.acme.budgeting.v1.ValidateBudgetLineResponse;
import com.acme.collections.v1.BatchCreateCollectionCasesRequest;
import com.acme.collections.v1.BatchCreateCollectionCasesResponse;
import com.acme.collections.v1.CollectionCaseServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
import com.acme.compliance.v1.QuoteComplianceCaseRequest;
import com.acme.compliance.v1.QuoteComplianceCaseResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.ledger.v1.ListLedgerAccountVersionsRequest;
import com.acme.ledger.v1.ListLedgerAccountVersionsResponse;
import com.acme.maintenance.v1.QuoteWorkOrderRequest;
import com.acme.maintenance.v1.QuoteWorkOrderResponse;
import com.acme.maintenance.v1.RecalculateWorkOrdersRequest;
import com.acme.maintenance.v1.RecalculateWorkOrdersResponse;
import com.acme.maintenance.v1.WorkOrder;
import com.acme.maintenance.v1.WorkOrderCalculationServiceGrpc;
import com.acme.maintenance.v1.WorkOrderChannelOrigin;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.venues.v1.ScoreVenueRequest;
import com.acme.venues.v1.ScoreVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Checks budget lines against the rules before anything acts on them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class BudgetLineValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(BudgetLineValidationApplication.class);

    private final AdmissionCalculationServiceGrpc.AdmissionCalculationServiceBlockingStub admissionsAdmissionCalculationServiceBlocking;

    private final AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking;

    private final AuditTrailSyncServiceGrpc.AuditTrailSyncServiceBlockingStub auditAuditTrailSyncServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final RetentionPolicyValidationServiceGrpc.RetentionPolicyValidationServiceBlockingStub auditRetentionPolicyValidationServiceBlocking;

    private final BudgetLineCalculationServiceGrpc.BudgetLineCalculationServiceBlockingStub budgetingBudgetLineCalculationServiceBlocking;

    private final ChannelRegistry channels;

    private final CollectionCaseServiceGrpc.CollectionCaseServiceStub collectionsCollectionCaseServiceAsync;

    private final ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceBlockingStub complianceComplianceCaseCalculationServiceBlocking;

    private final LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking;

    private final WorkOrderCalculationServiceGrpc.WorkOrderCalculationServiceBlockingStub maintenanceWorkOrderCalculationServiceBlocking;

    private final BudgetingRepository repository;

    private final VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking;

    public BudgetLineValidationApplication(
            AdmissionCalculationServiceGrpc.AdmissionCalculationServiceBlockingStub admissionsAdmissionCalculationServiceBlocking,
            AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking,
            AuditTrailSyncServiceGrpc.AuditTrailSyncServiceBlockingStub auditAuditTrailSyncServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            RetentionPolicyValidationServiceGrpc.RetentionPolicyValidationServiceBlockingStub auditRetentionPolicyValidationServiceBlocking,
            BudgetLineCalculationServiceGrpc.BudgetLineCalculationServiceBlockingStub budgetingBudgetLineCalculationServiceBlocking,
            ChannelRegistry channels,
            CollectionCaseServiceGrpc.CollectionCaseServiceStub collectionsCollectionCaseServiceAsync,
            ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceBlockingStub complianceComplianceCaseCalculationServiceBlocking,
            LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking,
            WorkOrderCalculationServiceGrpc.WorkOrderCalculationServiceBlockingStub maintenanceWorkOrderCalculationServiceBlocking,
            BudgetingRepository repository,
            VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking) {
        this.admissionsAdmissionCalculationServiceBlocking = admissionsAdmissionCalculationServiceBlocking;
        this.auditAuditEventValidationServiceBlocking = auditAuditEventValidationServiceBlocking;
        this.auditAuditTrailSyncServiceBlocking = auditAuditTrailSyncServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.auditRetentionPolicyValidationServiceBlocking = auditRetentionPolicyValidationServiceBlocking;
        this.budgetingBudgetLineCalculationServiceBlocking = budgetingBudgetLineCalculationServiceBlocking;
        this.channels = channels;
        this.collectionsCollectionCaseServiceAsync = collectionsCollectionCaseServiceAsync;
        this.complianceComplianceCaseCalculationServiceBlocking = complianceComplianceCaseCalculationServiceBlocking;
        this.ledgerLedgerAccountHistoryServiceBlocking = ledgerLedgerAccountHistoryServiceBlocking;
        this.maintenanceWorkOrderCalculationServiceBlocking = maintenanceWorkOrderCalculationServiceBlocking;
        this.repository = repository;
        this.venuesVenueValidationServiceBlocking = venuesVenueValidationServiceBlocking;
    }

    /**
     * Validates a budget line.
     */
    public ValidateBudgetLineResponse validateBudgetLine(ValidateBudgetLineRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateBudgetLine")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudgetLine(request.getContext().getTenantId(), request.getBudgetLine());

        var outbound0 = JsonCodec.roundTrip(ScoreVenueRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScoreVenueRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(request.getBudgetLine().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build(), ScoreVenueRequest.class);
        var reply0 = venuesVenueValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreVenue(outbound0);

        var outbound1 = QuoteComplianceCaseRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getBudgetLine().getAudit().getCreatedBy())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var command1 = new DispatchCommand<>("acme.compliance.v1.ComplianceCaseCalculationService/QuoteComplianceCase", outbound1);
        var reply1 = complianceComplianceCaseCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteComplianceCase(command1.payload());

        var outbound2 = CalculateBudgetLineRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getBudgetLine().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CalculateBudgetLineRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(switch (reply0.getVenue().getLocatorCase()) {
                                    case ADDRESS -> reply0.getVenue().getAddress().getRegion();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply2 = budgetingBudgetLineCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateBudgetLine(outbound2);

        var builder3 = SimulateBudgetLineRequest.newBuilder();
        builder3.setBudgetLine(BudgetLine.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setUpdatedAt(reply2.getBudgetLine().getAudit().getUpdatedAt())
                        .build())
                .build());
        builder3.setContext(request.getContext());
        var outbound3 = builder3.build();
        var reply3 = budgetingBudgetLineCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateBudgetLine(outbound3);

        var outbound4 = BatchCreateCollectionCasesRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateCollectionCasesRequest.StreamHeader.newBuilder()
                        .setSource(request.getBudgetLine().getBudgetLineId())
                        .setSubmittedBy(request.getDetails().getContact())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(BatchCreateCollectionCasesResponse.getDefaultInstance());
        var input4 = collectionsCollectionCaseServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateCollectionCases(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();

        var outbound5 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getBudgetLine().getAudit().getCreatedAt())
                                .setCreatedBy(request.getBudgetLine().getAudit().getCreatedBy())
                                .setEtag(request.getBudgetLine().getAudit().getEtag())
                                .setUpdatedAt(request.getBudgetLine().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector5 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input5 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector5);
        input5.onNext(outbound5);
        input5.onCompleted();
        var reply5 = collector5.await();
        log.atDebug().addKeyValue("auditValue", request.getBudgetLine().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateBudgetLine", request.getBudgetLine().getNumber());
        return ValidateBudgetLineResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply2.getBudgetLine().getBillingAddress().getCountryCode())
                                .setLocality(reply0.getVenue().getAddress().getLocality())
                                .build())
                        .setBudgetLineId(request.getBudgetLineId())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setDisplayName(reply0.getVenue().getOwner().getDisplayName())
                                .setSupplierId(reply3.getBudgetLine().getCounterparty().getSupplierId())
                                .build())
                        .setIssuedAt(request.getBudgetLine().getIssuedAt())
                        .setTotal(reply1.getBreakdown().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .setStatus(reply3.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Previews a budget line.
     */
    public PreviewBudgetLineResponse previewBudgetLine(PreviewBudgetLineRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewBudgetLine")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudgetLine(request.getContext().getTenantId(), request.getBudgetLine());

        var outbound0 = RecalculateWorkOrdersRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecalculateWorkOrdersRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getBudgetLine().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .setWorkOrder(WorkOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getBudgetLine().getAudit().getEtag())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .build();
        Function<RecalculateWorkOrdersRequest, RecalculateWorkOrdersResponse> operation0 = maintenanceWorkOrderCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::recalculateWorkOrders;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = QuoteWorkOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(QuoteWorkOrderRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getBudgetLine().getBillingAddress().getRegion())
                                .build())
                        .build())
                .setWorkOrder(WorkOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getWorkOrder().getAudit().getEtag())
                                .build())
                        .setChannel(WorkOrderChannelOrigin.newBuilder()
                                .setChannel(reply0.getWorkOrder().getChannel().getChannel())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply0.getWorkOrder().getAssignee().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = maintenanceWorkOrderCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteWorkOrder(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getBudgetLine().getBudgetLineId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewBudgetLine", request.getBudgetLine().getBudgetLineId());
        return PreviewBudgetLineResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBudgetLineId(request.getBudgetLineId())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(reply0.getWorkOrder().getAssignee().getEmail())
                                .build())
                        .setNotes(request.getBudgetLine().getNotes())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Checks a budget line.
     */
    public CheckBudgetLineEligibilityResponse checkBudgetLineEligibility(CheckBudgetLineEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckBudgetLineEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudgetLine(request.getContext().getTenantId(), request.getBudgetLine());

        var outbound0 = CalculateForecastRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CalculateForecastRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getBudgetLine().getBillingAddress().getRegion())
                                .build())
                        .build())
                .setForecast(Forecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getBudgetLine().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getBudgetLine().getBillingAddress().getCountryCode())
                                .build())
                        .setCounterparty(BudgetRef.newBuilder()
                                .setDisplayName(request.getBudgetLine().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.budgeting.v1.ForecastCalculationService");
        var serviceName0 = "acme.budgeting.v1" + "." + "ForecastCalculationService";
        var methodName0 = "Cal" + "culateForecast";
        var descriptor0 = MethodDescriptor.<CalculateForecastRequest, CalculateForecastResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(CalculateForecastRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(CalculateForecastResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = ListAudiencesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAudiencesRequest.Filter.newBuilder()
                        .setRegion(request.getBudgetLine().getBillingAddress().getRegion())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.audiences.v1.AudienceService");
        var selected1 = AudienceServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.listAudiences(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getBudgetLine().getBudgetLineId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckBudgetLineEligibility", request.getBudgetLine().getBudgetLineId());
        return CheckBudgetLineEligibilityResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getForecast().getAudit().getUpdatedAt())
                                .setUpdatedBy(reply0.getForecast().getAudit().getUpdatedBy())
                                .build())
                        .setBudgetLineId(request.getBudgetLineId())
                        .setIssuedAt(request.getBudgetLine().getIssuedAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Normalizes a budget line.
     */
    public NormalizeBudgetLineResponse normalizeBudgetLine(NormalizeBudgetLineRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeBudgetLine")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudgetLine(request.getContext().getTenantId(), request.getBudgetLine());

        var builder0 = LintAuditEventsRequest.newBuilder();
        builder0.setAuditEvent(AuditEvent.newBuilder()
                .setDetail(Any.pack(request.getContext()))
                .setSubject(AuditTrailRef.newBuilder()
                        .setDisplayName(request.getBudgetLine().getCounterparty().getDisplayName())
                        .build())
                .build());
        builder0.setContext(request.getContext());
        builder0.setDetails(LintAuditEventsRequest.Details.newBuilder()
                .setContact(ContactInfo.newBuilder()
                        .setName(request.getDetails().getContact().getName())
                        .setPhone(request.getDetails().getContact().getPhone())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = auditAuditEventValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintAuditEvents(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getBudgetLine().getBudgetLineId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeBudgetLine", request.getBudgetLine().getBudgetLineId());
        return NormalizeBudgetLineResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBudgetLineId(request.getBudgetLineId())
                        .setNotes(request.getBudgetLine().getNotes())
                        .build())
                .setOutcome(NormalizeBudgetLineResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .build();
    }

    /**
     * Explains a budget line.
     */
    public ExplainBudgetLineDecisionResponse explainBudgetLineDecision(ExplainBudgetLineDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainBudgetLineDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudgetLine(request.getContext().getTenantId(), request.getBudgetLine());

        var outbound0 = ReconcileAuditTrailsRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getBudgetLine().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getBudgetLine().getBillingAddress().getLinesList())
                                .setPostalCode(request.getBudgetLine().getBillingAddress().getPostalCode())
                                .build())
                        .setCard(AuditTrailCardSettlement.newBuilder()
                                .setNetwork(switch (request.getBudgetLine().getSettlementCase()) {
                                    case CARD -> request.getBudgetLine().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ReconcileAuditTrailsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var reply0 = auditAuditTrailSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).reconcileAuditTrails(outbound0);

        var outbound1 = ListLedgerAccountVersionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListLedgerAccountVersionsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getAuditTrail().getIssuedAt())
                        .putAllLabels(reply0.getAuditTrail().getMetadataMap())
                        .setOwnerId(reply0.getAuditTrail().getCounterparty().getRetentionPolicyId())
                        .setRegion(request.getBudgetLine().getBillingAddress().getRegion())
                        .build())
                .build();
        var adapter1 = new Function<ListLedgerAccountVersionsRequest, ListLedgerAccountVersionsResponse>() {
            @Override
            public ListLedgerAccountVersionsResponse apply(ListLedgerAccountVersionsRequest input) {
                return ledgerLedgerAccountHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listLedgerAccountVersions(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);

        var outbound2 = ValidateAuditEventRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .build())
                .setContext(request.getContext())
                .setDetails(ValidateAuditEventRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .setName(request.getDetails().getContact().getName())
                                .build())
                        .build())
                .build();
        var reply2 = auditAuditEventValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateAuditEvent(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getBudgetLine().getBudgetLineId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainBudgetLineDecision", request.getBudgetLine().getBudgetLineId());
        return ExplainBudgetLineDecisionResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBudgetLineId(request.getBudgetLineId())
                        .addLines(BudgetLineLine.newBuilder()
                                .putAllAttributes(reply2.getAuditEvent().getAttributesMap())
                                .build())
                        .setNumber(request.getBudgetLine().getNumber())
                        .build())
                .setOutcome(ExplainBudgetLineDecisionResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply2.getOutcome().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Scores a budget line.
     */
    public ScoreBudgetLineResponse scoreBudgetLine(ScoreBudgetLineRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreBudgetLine")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudgetLine(request.getContext().getTenantId(), request.getBudgetLine());

        var outbound0 = ScoreVenueRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScoreVenueRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getDetails().getDestination().getLocality())
                                .build())
                        .build())
                .setVenue(Venue.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getBudgetLine().getBillingAddress().getCountryCode())
                                .setPostalCode(request.getDetails().getDestination().getPostalCode())
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .build())
                        .build())
                .build();
        Function<ScoreVenueRequest, ScoreVenueResponse> operation0 = venuesVenueValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::scoreVenue;
        var reply0 = operation0.apply(outbound0);

        var results1 = new ArrayList<ScoreRetentionPolicyResponse>();
        for (var element : request.getBudgetLine().getLinesList()) {
            ScoreRetentionPolicyRequest itemRequest = ScoreRetentionPolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setDescription(element.getDescription())
                        .setDiscount(RetentionPolicyDiscountAction.newBuilder()
                                .setAmount(element.getTax().getAmount())
                                .build())
                        .build())
                .build();
            results1.add(auditRetentionPolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreRetentionPolicy(itemRequest));
        }
        var reply1 = results1.isEmpty() ? ScoreRetentionPolicyResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = CalculateAdmissionRequest.newBuilder()
                .setAdmission(Admission.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(GateScanRef.newBuilder()
                                .setDisplayName(reply0.getVenue().getOwner().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CalculateAdmissionRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(switch (reply0.getVenue().getLocatorCase()) {
                                    case ADDRESS -> reply0.getVenue().getAddress().getLocality();
                                    default -> "";
                                })
                                .setPostalCode(request.getBudgetLine().getBillingAddress().getPostalCode())
                                .setRecipient(switch (reply0.getVenue().getLocatorCase()) {
                                    case ADDRESS -> reply0.getVenue().getAddress().getRecipient();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply2 = admissionsAdmissionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateAdmission(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getBudgetLine().getBudgetLineId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreBudgetLine", request.getBudgetLine().getBudgetLineId());
        return ScoreBudgetLineResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBudgetLineId(request.getBudgetLineId())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(reply0.getVenue().getOwner().getEmail())
                                .build())
                        .setNumber(request.getBudgetLine().getNumber())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .build())
                .build();
    }
}
