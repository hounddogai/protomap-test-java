package com.acme.finance.domains.insurance;

import com.acme.accounting.v1.Accrual;
import com.acme.accounting.v1.CheckAccrualEligibilityRequest;
import com.acme.accounting.v1.CheckAccrualEligibilityResponse;
import com.acme.audit.v1.AuditEventHistoryServiceGrpc;
import com.acme.audit.v1.ListAuditEventVersionsRequest;
import com.acme.audit.v1.ListAuditEventVersionsResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastBankSettlement;
import com.acme.budgeting.v1.ForecastCalculationServiceGrpc;
import com.acme.budgeting.v1.QuoteForecastRequest;
import com.acme.budgeting.v1.QuoteForecastResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.ExplainRenewalDecisionRequest;
import com.acme.contracts.v1.ExplainRenewalDecisionResponse;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.RenewalParticipant;
import com.acme.contracts.v1.RenewalValidationServiceGrpc;
import com.acme.customers.v1.CalculateCustomerSegmentRequest;
import com.acme.customers.v1.CalculateCustomerSegmentResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerSegmentCondition;
import com.acme.customers.v1.HouseholdQueryServiceGrpc;
import com.acme.customers.v1.NormalizeConsentRecordRequest;
import com.acme.customers.v1.NormalizeConsentRecordResponse;
import com.acme.customers.v1.ResolveHouseholdsRequest;
import com.acme.customers.v1.ResolveHouseholdsResponse;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.documents.v1.PreviewDocumentRequest;
import com.acme.documents.v1.PreviewDocumentResponse;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ApproveCoverageRequest;
import com.acme.insurance.v1.ApproveCoverageResponse;
import com.acme.insurance.v1.CommentPolicyApprovalRequest;
import com.acme.insurance.v1.CommentPolicyApprovalResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.DelegateCoverageApprovalRequest;
import com.acme.insurance.v1.DelegateCoverageApprovalResponse;
import com.acme.insurance.v1.EscalateCoverageRequest;
import com.acme.insurance.v1.EscalateCoverageResponse;
import com.acme.insurance.v1.ListPendingCoveragesRequest;
import com.acme.insurance.v1.ListPendingCoveragesResponse;
import com.acme.insurance.v1.PausePolicyWorkflowRequest;
import com.acme.insurance.v1.PausePolicyWorkflowResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyApprovalServiceGrpc;
import com.acme.insurance.v1.PolicyWorkflowServiceGrpc;
import com.acme.insurance.v1.RecallCoverageRequest;
import com.acme.insurance.v1.RecallCoverageResponse;
import com.acme.insurance.v1.RecallPolicyRequest;
import com.acme.insurance.v1.RecallPolicyResponse;
import com.acme.insurance.v1.RejectCoverageRequest;
import com.acme.insurance.v1.RejectCoverageResponse;
import com.acme.insurance.v1.SubmitCoverageRequest;
import com.acme.insurance.v1.SubmitCoverageResponse;
import com.acme.risk.v1.CheckStressTestEligibilityRequest;
import com.acme.risk.v1.CheckStressTestEligibilityResponse;
import com.acme.risk.v1.LintStressTestsRequest;
import com.acme.risk.v1.LintStressTestsResponse;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestCardSettlement;
import com.acme.risk.v1.StressTestLine;
import com.acme.risk.v1.StressTestLineTax;
import com.acme.risk.v1.StressTestValidationServiceGrpc;
import com.acme.suppliers.v1.ExplainSupplierRiskDecisionRequest;
import com.acme.suppliers.v1.ExplainSupplierRiskDecisionResponse;
import com.acme.suppliers.v1.NormalizeSupplierRatingRequest;
import com.acme.suppliers.v1.NormalizeSupplierRatingResponse;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingCardSettlement;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.SupplierRiskValidationServiceGrpc;
import com.google.protobuf.Any;
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
 * Routes coverages through approval, with escalation and delegation. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class CoverageApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(CoverageApprovalApplication.class);

    private final AuditEventHistoryServiceGrpc.AuditEventHistoryServiceBlockingStub auditAuditEventHistoryServiceBlocking;

    private final ForecastCalculationServiceGrpc.ForecastCalculationServiceBlockingStub budgetingForecastCalculationServiceBlocking;

    private final RenewalValidationServiceGrpc.RenewalValidationServiceBlockingStub contractsRenewalValidationServiceBlocking;

    private final ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceBlockingStub customersConsentRecordValidationServiceBlocking;

    private final CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking;

    private final HouseholdQueryServiceGrpc.HouseholdQueryServiceFutureStub customersHouseholdQueryServiceFuture;

    private final DocumentValidationServiceGrpc.DocumentValidationServiceBlockingStub documentsDocumentValidationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final PolicyApprovalServiceGrpc.PolicyApprovalServiceBlockingStub insurancePolicyApprovalServiceBlocking;

    private final PolicyWorkflowServiceGrpc.PolicyWorkflowServiceBlockingStub insurancePolicyWorkflowServiceBlocking;

    private final InsuranceRepository repository;

    private final StressTestValidationServiceGrpc.StressTestValidationServiceBlockingStub riskStressTestValidationServiceBlocking;

    private final SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking;

    private final SupplierRiskValidationServiceGrpc.SupplierRiskValidationServiceBlockingStub suppliersSupplierRiskValidationServiceBlocking;

    public CoverageApprovalApplication(
            AuditEventHistoryServiceGrpc.AuditEventHistoryServiceBlockingStub auditAuditEventHistoryServiceBlocking,
            ForecastCalculationServiceGrpc.ForecastCalculationServiceBlockingStub budgetingForecastCalculationServiceBlocking,
            RenewalValidationServiceGrpc.RenewalValidationServiceBlockingStub contractsRenewalValidationServiceBlocking,
            ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceBlockingStub customersConsentRecordValidationServiceBlocking,
            CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking,
            HouseholdQueryServiceGrpc.HouseholdQueryServiceFutureStub customersHouseholdQueryServiceFuture,
            DocumentValidationServiceGrpc.DocumentValidationServiceBlockingStub documentsDocumentValidationServiceBlocking,
            ApplicationEventPublisher events,
            PolicyApprovalServiceGrpc.PolicyApprovalServiceBlockingStub insurancePolicyApprovalServiceBlocking,
            PolicyWorkflowServiceGrpc.PolicyWorkflowServiceBlockingStub insurancePolicyWorkflowServiceBlocking,
            InsuranceRepository repository,
            StressTestValidationServiceGrpc.StressTestValidationServiceBlockingStub riskStressTestValidationServiceBlocking,
            SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking,
            SupplierRiskValidationServiceGrpc.SupplierRiskValidationServiceBlockingStub suppliersSupplierRiskValidationServiceBlocking) {
        this.auditAuditEventHistoryServiceBlocking = auditAuditEventHistoryServiceBlocking;
        this.budgetingForecastCalculationServiceBlocking = budgetingForecastCalculationServiceBlocking;
        this.contractsRenewalValidationServiceBlocking = contractsRenewalValidationServiceBlocking;
        this.customersConsentRecordValidationServiceBlocking = customersConsentRecordValidationServiceBlocking;
        this.customersCustomerSegmentCalculationServiceBlocking = customersCustomerSegmentCalculationServiceBlocking;
        this.customersHouseholdQueryServiceFuture = customersHouseholdQueryServiceFuture;
        this.documentsDocumentValidationServiceBlocking = documentsDocumentValidationServiceBlocking;
        this.events = events;
        this.insurancePolicyApprovalServiceBlocking = insurancePolicyApprovalServiceBlocking;
        this.insurancePolicyWorkflowServiceBlocking = insurancePolicyWorkflowServiceBlocking;
        this.repository = repository;
        this.riskStressTestValidationServiceBlocking = riskStressTestValidationServiceBlocking;
        this.suppliersSupplierRatingValidationServiceBlocking = suppliersSupplierRatingValidationServiceBlocking;
        this.suppliersSupplierRiskValidationServiceBlocking = suppliersSupplierRiskValidationServiceBlocking;
    }

    public record ApproveCoverageStage0Requested(CheckStressTestEligibilityRequest request, CompletableFuture<CheckStressTestEligibilityResponse> completion, io.grpc.Context context) {}

    public record ListPendingCoveragesStage0Requested(CheckAccrualEligibilityRequest request, CompletableFuture<CheckAccrualEligibilityResponse> completion, io.grpc.Context context) {}

    /**
     * Submits a coverage.
     */
    public SubmitCoverageResponse submitCoverage(SubmitCoverageRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitCoverage")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCoverage(request.getContext().getTenantId(), request.getCoverage());

        List<CalculateCustomerSegmentResponse> results0 = request.getCoverage().getConditionsList().stream()
                .map(element -> CalculateCustomerSegmentRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .addConditions(CustomerSegmentCondition.newBuilder()
                                .setAttribute(element.getAttribute())
                                .setNegate(element.getNegate())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(customersCustomerSegmentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateCustomerSegment(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? CalculateCustomerSegmentResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = NormalizeConsentRecordRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .build())
                .setContext(request.getContext())
                .setDetails(NormalizeConsentRecordRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = customersConsentRecordValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeConsentRecord(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getCoverage().getCoverageId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitCoverage", request.getCoverage().getCoverageId());
        return SubmitCoverageResponse.newBuilder()
                .setCoverage(Coverage.newBuilder()
                        .setCoverageId(request.getCoverageId())
                        .setDescription(request.getCoverage().getDescription())
                        .build())
                .setOutcome(SubmitCoverageResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply1.getOutcome().getNextStepsList())
                        .build())
                .build();
    }

    /**
     * Approves a coverage.
     */
    public ApproveCoverageResponse approveCoverage(ApproveCoverageRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveCoverage")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCoverage(request.getContext().getTenantId(), request.getCoverage());

        var outbound0 = CheckStressTestEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setStressTest(StressTest.newBuilder()
                        .addLines(StressTestLine.newBuilder()
                                .setTax(StressTestLineTax.newBuilder()
                                        .setAmount(request.getCoverage().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<CheckStressTestEligibilityResponse>();
        events.publishEvent(new ApproveCoverageStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = ListAuditEventVersionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAuditEventVersionsRequest.Filter.newBuilder()
                        .setRegion(reply0.getStressTest().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply1 = auditAuditEventHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listAuditEventVersions(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getCoverage().getCoverageId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveCoverage", request.getCoverage().getCoverageId());
        return ApproveCoverageResponse.newBuilder()
                .setCoverage(Coverage.newBuilder()
                        .setCoverageId(request.getCoverageId())
                        .setEffective(request.getCoverage().getEffective())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getStressTest().getCounterparty().getEmail())
                                .build())
                        .build())
                .setOutcome(ApproveCoverageResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Rejects a coverage.
     */
    public RejectCoverageResponse rejectCoverage(RejectCoverageRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectCoverage")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCoverage(request.getContext().getTenantId(), request.getCoverage());

        var outbound0 = CommentPolicyApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CommentPolicyApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getCoverage().getOwner().getEmail())
                                .build())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getCoverage().getAudit().getEtag())
                                .setUpdatedAt(request.getCoverage().getAudit().getUpdatedAt())
                                .setUpdatedBy(request.getCoverage().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> insurancePolicyApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentPolicyApproval(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getCoverage().getCoverageId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectCoverage", request.getCoverage().getCoverageId());
        return RejectCoverageResponse.newBuilder()
                .setCoverage(Coverage.newBuilder()
                        .setCoverageId(request.getCoverageId())
                        .setDescription(request.getCoverage().getDescription())
                        .build())
                .setOutcome(RejectCoverageResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .build();
    }

    /**
     * Escalates a coverage.
     */
    public EscalateCoverageResponse escalateCoverage(EscalateCoverageRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateCoverage")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCoverage(request.getContext().getTenantId(), request.getCoverage());

        var outbound0 = PausePolicyWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PausePolicyWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getCoverage().getAudit().getUpdatedAt())
                                .setUpdatedBy(request.getCoverage().getAudit().getUpdatedBy())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getCoverage().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> insurancePolicyWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pausePolicyWorkflow(value));

        var outbound1 = ExplainSupplierRiskDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRisk(SupplierRisk.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getCoverage().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        Function<ExplainSupplierRiskDecisionRequest, ExplainSupplierRiskDecisionResponse> operation1 = suppliersSupplierRiskValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::explainSupplierRiskDecision;
        var reply1 = operation1.apply(outbound1);

        var outbound2 = LintStressTestsRequest.newBuilder()
                .setContext(request.getContext())
                .setStressTest(StressTest.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getSupplierRisk().getAudit().getEtag())
                                .build())
                        .setCard(StressTestCardSettlement.newBuilder()
                                .setToken(switch (reply0.getPolicy().getFundingCase()) {
                                    case CARD -> reply0.getPolicy().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply1.getSupplierRisk().getRequester().getEmail())
                                .build())
                        .addLines(StressTestLine.newBuilder()
                                .setAmount(request.getCoverage().getDiscount().getAmount())
                                .build())
                        .build())
                .build();
        var reply2 = riskStressTestValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintStressTests(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getCoverage().getCoverageId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateCoverage", request.getCoverage().getCoverageId());
        return EscalateCoverageResponse.newBuilder()
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply2.getStressTest().getAudit().getCreatedBy())
                                .setEtag(reply0.getPolicy().getAudit().getEtag())
                                .build())
                        .setCoverageId(request.getCoverageId())
                        .setName(request.getCoverage().getName())
                        .build())
                .setOutcome(EscalateCoverageResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply1.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Lists coverages a page at a time.
     */
    public ListPendingCoveragesResponse listPendingCoverages(ListPendingCoveragesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingCoverages")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Coverage", request.getContext().getTraceId(), "ListPendingCoverages");

        var outbound0 = CheckAccrualEligibilityRequest.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var completion0 = new CompletableFuture<CheckAccrualEligibilityResponse>();
        events.publishEvent(new ListPendingCoveragesStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = QuoteForecastRequest.newBuilder()
                .setContext(request.getContext())
                .setForecast(Forecast.newBuilder()
                        .setBank(ForecastBankSettlement.newBuilder()
                                .setBic(switch (reply0.getAccrual().getSettlementCase()) {
                                    case BANK -> reply0.getAccrual().getBank().getBic();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = budgetingForecastCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteForecast(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingCoverages", request.getPage().getPageToken());
        return ListPendingCoveragesResponse.newBuilder()
                .addCoverages(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getAccrual().getAudit().getCreatedBy())
                                .setEtag(reply0.getAccrual().getAudit().getEtag())
                                .setUpdatedAt(reply1.getForecast().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
    }

    /**
     * Delegates a coverage.
     */
    public DelegateCoverageApprovalResponse delegateCoverageApproval(DelegateCoverageApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateCoverageApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCoverage(request.getContext().getTenantId(), request.getCoverage());

        var outbound0 = ResolveHouseholdsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending0 = customersHouseholdQueryServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).resolveHouseholds(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = ExplainRenewalDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setRenewal(Renewal.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getCoverage().getAudit().getUpdatedAt())
                                .build())
                        .addParticipants(RenewalParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getCoverage().getOwner().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        RpcPort<ExplainRenewalDecisionRequest, ExplainRenewalDecisionResponse> port1 = RpcPort.adapt(contractsRenewalValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::explainRenewalDecision);
        var reply1 = port1.exchange(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getCoverage().getCoverageId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateCoverageApproval", request.getCoverage().getCoverageId());
        return DelegateCoverageApprovalResponse.newBuilder()
                .setCoverage(Coverage.newBuilder()
                        .setCoverageId(request.getCoverageId())
                        .setName(request.getCoverage().getName())
                        .build())
                .setOutcome(DelegateCoverageApprovalResponse.Outcome.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Recalls a coverage.
     */
    public RecallCoverageResponse recallCoverage(RecallCoverageRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallCoverage")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCoverage(request.getContext().getTenantId(), request.getCoverage());

        var outbound0 = RecallPolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecallPolicyRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .build();
        var reply0 = insurancePolicyApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallPolicy(outbound0);

        var outbound1 = NormalizeSupplierRatingRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setCard(SupplierRatingCardSettlement.newBuilder()
                                .setNetwork(reply0.getPolicy().getCard().getNetwork())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply0.getPolicy().getContact().getEmail())
                                .build())
                        .build())
                .build();
        Function<NormalizeSupplierRatingRequest, NormalizeSupplierRatingResponse> operation1 = suppliersSupplierRatingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::normalizeSupplierRating;
        var reply1 = operation1.apply(outbound1);

        var outbound2 = JsonCodec.roundTrip(PreviewDocumentRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewDocumentRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getPolicy().getContact().getEmail())
                                .build())
                        .build())
                .setDocument(Document.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getSupplierRating().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build(), PreviewDocumentRequest.class);
        var reply2 = documentsDocumentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewDocument(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getCoverage().getCoverageId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallCoverage", request.getCoverage().getCoverageId());
        return RecallCoverageResponse.newBuilder()
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getSupplierRating().getAudit().getUpdatedBy())
                                .build())
                        .setCoverageId(request.getCoverageId())
                        .putAllParameters(request.getCoverage().getParametersMap())
                        .build())
                .setOutcome(RecallCoverageResponse.Outcome.newBuilder()
                        .setReferenceId(reply1.getOutcome().getReferenceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }
}
