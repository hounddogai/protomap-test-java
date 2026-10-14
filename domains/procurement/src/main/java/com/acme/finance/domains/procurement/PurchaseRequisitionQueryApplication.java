package com.acme.finance.domains.procurement;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailLine;
import com.acme.audit.v1.AuditTrailSyncServiceGrpc;
import com.acme.audit.v1.ListAuditTrailConflictsRequest;
import com.acme.audit.v1.ListAuditTrailConflictsResponse;
import com.acme.audit.v1.PushAuditTrailRequest;
import com.acme.audit.v1.PushAuditTrailResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.NormalizePolicyRequest;
import com.acme.insurance.v1.NormalizePolicyResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyValidationServiceGrpc;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.RepaymentValidationServiceGrpc;
import com.acme.lending.v1.ValidateRepaymentRequest;
import com.acme.lending.v1.ValidateRepaymentResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesValidationServiceGrpc;
import com.acme.metrics.v1.NormalizeMetricSeriesRequest;
import com.acme.metrics.v1.NormalizeMetricSeriesResponse;
import com.acme.metrics.v1.ValidateMetricSeriesRequest;
import com.acme.metrics.v1.ValidateMetricSeriesResponse;
import com.acme.procurement.v1.BrowsePurchaseRequisitionsRequest;
import com.acme.procurement.v1.BrowsePurchaseRequisitionsResponse;
import com.acme.procurement.v1.CheckRfqEligibilityRequest;
import com.acme.procurement.v1.CheckRfqEligibilityResponse;
import com.acme.procurement.v1.CountPurchaseRequisitionsRequest;
import com.acme.procurement.v1.CountPurchaseRequisitionsResponse;
import com.acme.procurement.v1.ExplainPurchaseOrderDecisionRequest;
import com.acme.procurement.v1.ExplainPurchaseOrderDecisionResponse;
import com.acme.procurement.v1.ExportPurchaseRequisitionsRequest;
import com.acme.procurement.v1.ExportPurchaseRequisitionsResponse;
import com.acme.procurement.v1.FindPurchaseRequisitionsRequest;
import com.acme.procurement.v1.FindPurchaseRequisitionsResponse;
import com.acme.procurement.v1.LookupPurchaseRequisitionRequest;
import com.acme.procurement.v1.LookupPurchaseRequisitionResponse;
import com.acme.procurement.v1.PeekPurchaseRequisitionRequest;
import com.acme.procurement.v1.PeekPurchaseRequisitionResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderCardSettlement;
import com.acme.procurement.v1.PurchaseOrderValidationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionValidationServiceGrpc;
import com.acme.procurement.v1.ResolvePurchaseRequisitionsRequest;
import com.acme.procurement.v1.ResolvePurchaseRequisitionsResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqCalculationServiceGrpc;
import com.acme.procurement.v1.RfqValidationServiceGrpc;
import com.acme.procurement.v1.SimulateRfqRequest;
import com.acme.procurement.v1.SimulateRfqResponse;
import com.acme.procurement.v1.SuggestPurchaseRequisitionsRequest;
import com.acme.procurement.v1.SuggestPurchaseRequisitionsResponse;
import com.acme.procurement.v1.SummarizePurchaseRequisitionsRequest;
import com.acme.procurement.v1.SummarizePurchaseRequisitionsResponse;
import com.acme.procurement.v1.ValidatePurchaseRequisitionRequest;
import com.acme.procurement.v1.ValidatePurchaseRequisitionResponse;
import com.acme.risk.v1.RecalculateStressTestsRequest;
import com.acme.risk.v1.RecalculateStressTestsResponse;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestCalculationServiceGrpc;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Answers read-only questions about purchase requisitions, for screens and reports. Coordinates validation,
 * durable audit writes, and downstream operations.
 */
@Service
public class PurchaseRequisitionQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(PurchaseRequisitionQueryApplication.class);

    private final AuditTrailSyncServiceGrpc.AuditTrailSyncServiceBlockingStub auditAuditTrailSyncServiceBlocking;

    private final PolicyValidationServiceGrpc.PolicyValidationServiceBlockingStub insurancePolicyValidationServiceBlocking;

    private final RepaymentValidationServiceGrpc.RepaymentValidationServiceBlockingStub lendingRepaymentValidationServiceBlocking;

    private final MetricSeriesValidationServiceGrpc.MetricSeriesValidationServiceBlockingStub metricsMetricSeriesValidationServiceBlocking;

    private final PurchaseOrderValidationServiceGrpc.PurchaseOrderValidationServiceBlockingStub procurementPurchaseOrderValidationServiceBlocking;

    private final PurchaseRequisitionValidationServiceGrpc.PurchaseRequisitionValidationServiceBlockingStub procurementPurchaseRequisitionValidationServiceBlocking;

    private final RfqCalculationServiceGrpc.RfqCalculationServiceBlockingStub procurementRfqCalculationServiceBlocking;

    private final RfqValidationServiceGrpc.RfqValidationServiceBlockingStub procurementRfqValidationServiceBlocking;

    private final ProcurementRepository repository;

    private final StressTestCalculationServiceGrpc.StressTestCalculationServiceFutureStub riskStressTestCalculationServiceFuture;

    public PurchaseRequisitionQueryApplication(
            AuditTrailSyncServiceGrpc.AuditTrailSyncServiceBlockingStub auditAuditTrailSyncServiceBlocking,
            PolicyValidationServiceGrpc.PolicyValidationServiceBlockingStub insurancePolicyValidationServiceBlocking,
            RepaymentValidationServiceGrpc.RepaymentValidationServiceBlockingStub lendingRepaymentValidationServiceBlocking,
            MetricSeriesValidationServiceGrpc.MetricSeriesValidationServiceBlockingStub metricsMetricSeriesValidationServiceBlocking,
            PurchaseOrderValidationServiceGrpc.PurchaseOrderValidationServiceBlockingStub procurementPurchaseOrderValidationServiceBlocking,
            PurchaseRequisitionValidationServiceGrpc.PurchaseRequisitionValidationServiceBlockingStub procurementPurchaseRequisitionValidationServiceBlocking,
            RfqCalculationServiceGrpc.RfqCalculationServiceBlockingStub procurementRfqCalculationServiceBlocking,
            RfqValidationServiceGrpc.RfqValidationServiceBlockingStub procurementRfqValidationServiceBlocking,
            ProcurementRepository repository,
            StressTestCalculationServiceGrpc.StressTestCalculationServiceFutureStub riskStressTestCalculationServiceFuture) {
        this.auditAuditTrailSyncServiceBlocking = auditAuditTrailSyncServiceBlocking;
        this.insurancePolicyValidationServiceBlocking = insurancePolicyValidationServiceBlocking;
        this.lendingRepaymentValidationServiceBlocking = lendingRepaymentValidationServiceBlocking;
        this.metricsMetricSeriesValidationServiceBlocking = metricsMetricSeriesValidationServiceBlocking;
        this.procurementPurchaseOrderValidationServiceBlocking = procurementPurchaseOrderValidationServiceBlocking;
        this.procurementPurchaseRequisitionValidationServiceBlocking = procurementPurchaseRequisitionValidationServiceBlocking;
        this.procurementRfqCalculationServiceBlocking = procurementRfqCalculationServiceBlocking;
        this.procurementRfqValidationServiceBlocking = procurementRfqValidationServiceBlocking;
        this.repository = repository;
        this.riskStressTestCalculationServiceFuture = riskStressTestCalculationServiceFuture;
    }

    /**
     * Returns a purchase requisition.
     */
    public LookupPurchaseRequisitionResponse lookupPurchaseRequisition(LookupPurchaseRequisitionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupPurchaseRequisition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseRequisition", request.getPurchaseRequisitionId(), "LookupPurchaseRequisition");

        var outbound0 = NormalizeMetricSeriesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeMetricSeriesRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> metricsMetricSeriesValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeMetricSeries(value)).orElseThrow();
        return LookupPurchaseRequisitionResponse.newBuilder()
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setPurchaseRequisitionId(request.getPurchaseRequisitionId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists purchase requisitions a page at a time.
     */
    public BrowsePurchaseRequisitionsResponse browsePurchaseRequisitions(BrowsePurchaseRequisitionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowsePurchaseRequisitions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseRequisition", request.getContext().getTraceId(), "BrowsePurchaseRequisitions");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowsePurchaseRequisitions", request.getPage().getPageToken());
        return BrowsePurchaseRequisitionsResponse.getDefaultInstance();
    }

    /**
     * Lists purchase requisitions a page at a time.
     */
    public FindPurchaseRequisitionsResponse findPurchaseRequisitions(FindPurchaseRequisitionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FindPurchaseRequisitions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseRequisition", request.getContext().getTraceId(), "FindPurchaseRequisitions");

        var builder0 = ValidatePurchaseRequisitionRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDetails(ValidatePurchaseRequisitionRequest.Details.newBuilder()
                .setContact(ContactInfo.newBuilder()
                        .setEmail(request.getContext().getActor().getEmail())
                        .build())
                .build());
        builder0.setPurchaseRequisition(PurchaseRequisition.newBuilder()
                .setBillingAddress(PostalAddress.newBuilder()
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = procurementPurchaseRequisitionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validatePurchaseRequisition(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "FindPurchaseRequisitions", request.getPage().getPageToken());
        return FindPurchaseRequisitionsResponse.newBuilder()
                .addPurchaseRequisitions(PurchaseRequisition.newBuilder()
                        .setBillingAddress(reply0.getPurchaseRequisition().getBillingAddress())
                        .build())
                .build();
    }

    /**
     * Counts purchase requisitions by status.
     */
    public CountPurchaseRequisitionsResponse countPurchaseRequisitions(CountPurchaseRequisitionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountPurchaseRequisitions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseRequisition", request.getContext().getTraceId(), "CountPurchaseRequisitions");

        var outbound0 = SimulateRfqRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SimulateRfqRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            SimulateRfqResponse fetch(SimulateRfqRequest input) {
                return procurementRfqCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateRfq(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = (ValidateMetricSeriesRequest.newBuilder()
                .setContext(request.getContext())
                .setMetricSeries(MetricSeries.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getRfq().getAudit().getEtag())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getRfq().getAssignee().getDisplayName())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply1 = metricsMetricSeriesValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateMetricSeries(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountPurchaseRequisitions", request.getPage().getPageToken());
        return CountPurchaseRequisitionsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns several purchase requisitions at once.
     */
    public ResolvePurchaseRequisitionsResponse resolvePurchaseRequisitions(ResolvePurchaseRequisitionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolvePurchaseRequisitions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseRequisition", request.getContext().getTraceId(), "ResolvePurchaseRequisitions");

        var outbound0 = ExplainPurchaseOrderDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainPurchaseOrderDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build();
        RpcPort<ExplainPurchaseOrderDecisionRequest, ExplainPurchaseOrderDecisionResponse> port0 = RpcPort.adapt(procurementPurchaseOrderValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::explainPurchaseOrderDecision);
        var reply0 = port0.exchange(outbound0);
        return ResolvePurchaseRequisitionsResponse.newBuilder()
                .addPurchaseRequisitions(PurchaseRequisition.newBuilder()
                        .setIssuedAt(reply0.getPurchaseOrder().getIssuedAt())
                        .build())
                .build();
    }

    /**
     * Streams every matching purchase requisition.
     */
    public ExportPurchaseRequisitionsResponse exportPurchaseRequisitions(ExportPurchaseRequisitionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExportPurchaseRequisitions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseRequisition", request.getContext().getTraceId(), "ExportPurchaseRequisitions");

        var outbound0 = ValidateRepaymentRequest.newBuilder()
                .setContext(request.getContext())
                .setRepayment(Repayment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getContext().getActor().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(lendingRepaymentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateRepayment(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = CheckRfqEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setRfq(Rfq.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(reply0.getRepayment().getOrganizer().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getRepayment().getAudit().getEtag())
                                .setUpdatedBy(reply0.getRepayment().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        final class Stage1Adapter {
            CheckRfqEligibilityResponse fetch(CheckRfqEligibilityRequest input) {
                return procurementRfqValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkRfqEligibility(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);

        var outbound2 = NormalizePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getRfq().getAudit().getUpdatedBy())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(reply1.getRfq().getRequester().getPreferredChannel())
                                .build())
                        .setHolder(CoverageRef.newBuilder()
                                .setDisplayName(switch (reply1.getRfq().getOriginCase()) {
                                    case CUSTOMER -> reply1.getRfq().getCustomer().getDisplayName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply2 = insurancePolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizePolicy(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExportPurchaseRequisitions", request.getFilter().getQuery());
        return ExportPurchaseRequisitionsResponse.newBuilder()
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getRfq().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getRepayment().getLocation().getLinesList())
                                .setRegion(reply0.getRepayment().getLocation().getRegion())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Summarizes a purchase requisition.
     */
    public SummarizePurchaseRequisitionsResponse summarizePurchaseRequisitions(SummarizePurchaseRequisitionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SummarizePurchaseRequisitions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseRequisition", request.getPurchaseRequisitionId(), "SummarizePurchaseRequisitions");

        var outbound0 = RecalculateStressTestsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setReason(request.getReason())
                .setStressTest(StressTest.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .putAllMetadata(request.getDetails().getFiltersMap())
                        .build())
                .build();
        var pending0 = riskStressTestCalculationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateStressTests(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = ExplainPurchaseOrderDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getStressTest().getBillingAddress().getLinesList())
                                .build())
                        .setCard(PurchaseOrderCardSettlement.newBuilder()
                                .setLast4(switch (reply0.getStressTest().getSettlementCase()) {
                                    case CARD -> reply0.getStressTest().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = procurementPurchaseOrderValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainPurchaseOrderDecision(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPurchaseRequisitionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SummarizePurchaseRequisitions", request.getPurchaseRequisitionId());
        return SummarizePurchaseRequisitionsResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Lists purchase requisitions a page at a time.
     */
    public SuggestPurchaseRequisitionsResponse suggestPurchaseRequisitions(SuggestPurchaseRequisitionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuggestPurchaseRequisitions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseRequisition", request.getContext().getTraceId(), "SuggestPurchaseRequisitions");

        var outbound0 = ListAuditTrailConflictsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAuditTrailConflictsRequest.Filter.newBuilder()
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .build())
                .build();
        Function<ListAuditTrailConflictsRequest, ListAuditTrailConflictsResponse> operation0 = auditAuditTrailSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::listAuditTrailConflicts;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = PushAuditTrailRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setDueAt(request.getFilter().getCreatedAfter())
                        .addLines(AuditTrailLine.newBuilder()
                                .putAllAttributes(reply0.getResult().getDetailsMap())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PushAuditTrailRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        final class Stage1Adapter {
            PushAuditTrailResponse fetch(PushAuditTrailRequest input) {
                return auditAuditTrailSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pushAuditTrail(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuggestPurchaseRequisitions", request.getPage().getPageToken());
        return SuggestPurchaseRequisitionsResponse.newBuilder()
                .setPageInfo(PageInfo.newBuilder()
                        .setNextPageToken(reply0.getPageInfo().getNextPageToken())
                        .build())
                .addPurchaseRequisitions(PurchaseRequisition.newBuilder()
                        .setTotal(reply1.getAuditTrail().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Returns a purchase requisition.
     */
    public PeekPurchaseRequisitionResponse peekPurchaseRequisition(PeekPurchaseRequisitionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PeekPurchaseRequisition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseRequisition", request.getPurchaseRequisitionId(), "PeekPurchaseRequisition");
        return PeekPurchaseRequisitionResponse.newBuilder()
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setPurchaseRequisitionId(request.getPurchaseRequisitionId())
                        .build())
                .build();
    }
}
