package com.acme.finance.domains.risk;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyValidationServiceGrpc;
import com.acme.authorization.v1.NormalizeAccessPolicyRequest;
import com.acme.authorization.v1.NormalizeAccessPolicyResponse;
import com.acme.banking.v1.FindStatementsRequest;
import com.acme.banking.v1.FindStatementsResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StandingOrderValidationServiceGrpc;
import com.acme.banking.v1.StatementQueryServiceGrpc;
import com.acme.banking.v1.ValidateStandingOrderRequest;
import com.acme.banking.v1.ValidateStandingOrderResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.BrowsePoliciesRequest;
import com.acme.insurance.v1.BrowsePoliciesResponse;
import com.acme.insurance.v1.ExportPoliciesRequest;
import com.acme.insurance.v1.ExportPoliciesResponse;
import com.acme.insurance.v1.NormalizePremiumRequest;
import com.acme.insurance.v1.NormalizePremiumResponse;
import com.acme.insurance.v1.PolicyQueryServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumLine;
import com.acme.insurance.v1.PremiumLineTax;
import com.acme.insurance.v1.PremiumValidationServiceGrpc;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryBankSettlement;
import com.acme.ledger.v1.JournalEntryHistoryServiceGrpc;
import com.acme.ledger.v1.JournalEntryLine;
import com.acme.ledger.v1.JournalEntryLineTax;
import com.acme.ledger.v1.RevertJournalEntryRequest;
import com.acme.ledger.v1.RevertJournalEntryResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralValidationServiceGrpc;
import com.acme.lending.v1.ExplainCollateralDecisionRequest;
import com.acme.lending.v1.ExplainCollateralDecisionResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationValidationServiceGrpc;
import com.acme.lending.v1.ValidateLoanApplicationRequest;
import com.acme.lending.v1.ValidateLoanApplicationResponse;
import com.acme.risk.v1.CalculateStressTestRequest;
import com.acme.risk.v1.CalculateStressTestResponse;
import com.acme.risk.v1.CheckExposureEligibilityRequest;
import com.acme.risk.v1.CheckExposureEligibilityResponse;
import com.acme.risk.v1.CheckRiskScenarioEligibilityRequest;
import com.acme.risk.v1.CheckRiskScenarioEligibilityResponse;
import com.acme.risk.v1.CheckStressTestEligibilityRequest;
import com.acme.risk.v1.CheckStressTestEligibilityResponse;
import com.acme.risk.v1.ExplainRiskScenarioDecisionRequest;
import com.acme.risk.v1.ExplainRiskScenarioDecisionResponse;
import com.acme.risk.v1.ExplainStressTestDecisionRequest;
import com.acme.risk.v1.ExplainStressTestDecisionResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureValidationServiceGrpc;
import com.acme.risk.v1.LintStressTestsRequest;
import com.acme.risk.v1.LintStressTestsResponse;
import com.acme.risk.v1.NormalizeRiskScenarioRequest;
import com.acme.risk.v1.NormalizeRiskScenarioResponse;
import com.acme.risk.v1.NormalizeStressTestRequest;
import com.acme.risk.v1.NormalizeStressTestResponse;
import com.acme.risk.v1.PreviewStressTestRequest;
import com.acme.risk.v1.PreviewStressTestResponse;
import com.acme.risk.v1.RecalculateStressTestsRequest;
import com.acme.risk.v1.RecalculateStressTestsResponse;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioValidationServiceGrpc;
import com.acme.risk.v1.ScoreStressTestRequest;
import com.acme.risk.v1.ScoreStressTestResponse;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestBankSettlement;
import com.acme.risk.v1.StressTestCalculationServiceGrpc;
import com.acme.risk.v1.ValidateRiskScenarioRequest;
import com.acme.risk.v1.ValidateRiskScenarioResponse;
import com.acme.risk.v1.ValidateStressTestRequest;
import com.acme.risk.v1.ValidateStressTestResponse;
import com.google.protobuf.Any;
import java.util.ArrayList;
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
 * Checks stress tests against the rules before anything acts on them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class StressTestValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(StressTestValidationApplication.class);

    private final AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final AccessPolicyValidationServiceGrpc.AccessPolicyValidationServiceBlockingStub authorizationAccessPolicyValidationServiceBlocking;

    private final StandingOrderValidationServiceGrpc.StandingOrderValidationServiceBlockingStub bankingStandingOrderValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final ApplicationEventPublisher events;

    private final PolicyQueryServiceGrpc.PolicyQueryServiceBlockingStub insurancePolicyQueryServiceBlocking;

    private final PremiumValidationServiceGrpc.PremiumValidationServiceBlockingStub insurancePremiumValidationServiceBlocking;

    private final JournalEntryHistoryServiceGrpc.JournalEntryHistoryServiceBlockingStub ledgerJournalEntryHistoryServiceBlocking;

    private final CollateralValidationServiceGrpc.CollateralValidationServiceBlockingStub lendingCollateralValidationServiceBlocking;

    private final RiskRepository repository;

    private final RiskScenarioValidationServiceGrpc.RiskScenarioValidationServiceBlockingStub riskRiskScenarioValidationServiceBlocking;

    private final StressTestCalculationServiceGrpc.StressTestCalculationServiceBlockingStub riskStressTestCalculationServiceBlocking;

    public StressTestValidationApplication(
            AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            AccessPolicyValidationServiceGrpc.AccessPolicyValidationServiceBlockingStub authorizationAccessPolicyValidationServiceBlocking,
            StandingOrderValidationServiceGrpc.StandingOrderValidationServiceBlockingStub bankingStandingOrderValidationServiceBlocking,
            ChannelRegistry channels,
            ApplicationEventPublisher events,
            PolicyQueryServiceGrpc.PolicyQueryServiceBlockingStub insurancePolicyQueryServiceBlocking,
            PremiumValidationServiceGrpc.PremiumValidationServiceBlockingStub insurancePremiumValidationServiceBlocking,
            JournalEntryHistoryServiceGrpc.JournalEntryHistoryServiceBlockingStub ledgerJournalEntryHistoryServiceBlocking,
            CollateralValidationServiceGrpc.CollateralValidationServiceBlockingStub lendingCollateralValidationServiceBlocking,
            RiskRepository repository,
            RiskScenarioValidationServiceGrpc.RiskScenarioValidationServiceBlockingStub riskRiskScenarioValidationServiceBlocking,
            StressTestCalculationServiceGrpc.StressTestCalculationServiceBlockingStub riskStressTestCalculationServiceBlocking) {
        this.auditAuditEventServiceAsync = auditAuditEventServiceAsync;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.authorizationAccessPolicyValidationServiceBlocking = authorizationAccessPolicyValidationServiceBlocking;
        this.bankingStandingOrderValidationServiceBlocking = bankingStandingOrderValidationServiceBlocking;
        this.channels = channels;
        this.events = events;
        this.insurancePolicyQueryServiceBlocking = insurancePolicyQueryServiceBlocking;
        this.insurancePremiumValidationServiceBlocking = insurancePremiumValidationServiceBlocking;
        this.ledgerJournalEntryHistoryServiceBlocking = ledgerJournalEntryHistoryServiceBlocking;
        this.lendingCollateralValidationServiceBlocking = lendingCollateralValidationServiceBlocking;
        this.repository = repository;
        this.riskRiskScenarioValidationServiceBlocking = riskRiskScenarioValidationServiceBlocking;
        this.riskStressTestCalculationServiceBlocking = riskStressTestCalculationServiceBlocking;
    }

    public record PreviewStressTestStage2Requested(ValidateRiskScenarioRequest request, CompletableFuture<ValidateRiskScenarioResponse> completion, io.grpc.Context context) {}

    /**
     * Validates a stress test.
     */
    public ValidateStressTestResponse validateStressTest(ValidateStressTestRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateStressTest")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStressTest(request.getContext().getTenantId(), request.getStressTest());

        var outbound0 = CalculateStressTestRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CalculateStressTestRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getStressTest().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<CalculateStressTestRequest, CalculateStressTestResponse>() {
            @Override
            public CalculateStressTestResponse apply(CalculateStressTestRequest input) {
                return riskStressTestCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateStressTest(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = RpcSupport.unpack(Any.pack(ValidateRiskScenarioRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply0.getStressTest().getCounterparty().getEmail())
                                .build())
                        .build())
                .build()), ValidateRiskScenarioRequest.class);
        var reply1 = riskRiskScenarioValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateRiskScenario(outbound1);

        var outbound2 = BatchCreateAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateAuditEventsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getContact())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(BatchCreateAuditEventsResponse.getDefaultInstance());
        var input2 = auditAuditEventServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateAuditEvents(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getStressTest().getAudit().getCreatedAt())
                                .setCreatedBy(request.getStressTest().getAudit().getCreatedBy())
                                .setEtag(request.getStressTest().getAudit().getEtag())
                                .setUpdatedAt(request.getStressTest().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input3 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getStressTest().getStressTestId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateStressTest", request.getStressTest().getStressTestId());
        return ValidateStressTestResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .setStressTest(StressTest.newBuilder()
                        .setDueAt(reply1.getRiskScenario().getSla().getDueAt())
                        .setNumber(request.getStressTest().getNumber())
                        .setStressTestId(request.getStressTestId())
                        .build())
                .build();
    }

    /**
     * Previews a stress test.
     */
    public PreviewStressTestResponse previewStressTest(PreviewStressTestRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewStressTest")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStressTest(request.getContext().getTenantId(), request.getStressTest());

        var outbound0 = BrowsePoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowsePoliciesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getStressTest().getIssuedAt())
                        .putAllLabels(request.getStressTest().getMetadataMap())
                        .setOwnerId(request.getStressTest().getCounterparty().getCustomerId())
                        .setRegion(request.getStressTest().getBillingAddress().getRegion())
                        .build())
                .build();
        Function<BrowsePoliciesRequest, BrowsePoliciesResponse> operation0 = insurancePolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::browsePolicies;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = ValidateLoanApplicationRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getStressTest().getBillingAddress().getPostalCode())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getStressTest().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.lending.v1.LoanApplicationValidationService");
        var selected1 = LoanApplicationValidationServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.validateLoanApplication(outbound1);

        var outbound2 = ValidateRiskScenarioRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply1.getLoanApplication().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var completion2 = new CompletableFuture<ValidateRiskScenarioResponse>();
        events.publishEvent(new PreviewStressTestStage2Requested(outbound2, completion2, io.grpc.Context.current()));
        var reply2 = completion2.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getStressTest().getStressTestId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewStressTest", request.getStressTest().getStressTestId());
        return PreviewStressTestResponse.newBuilder()
                .setOutcome(PreviewStressTestResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .build())
                .setStressTest(StressTest.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getLoanApplication().getBillingAddress().getCountryCode())
                                .build())
                        .setDueAt(request.getStressTest().getDueAt())
                        .setStressTestId(request.getStressTestId())
                        .build())
                .build();
    }

    /**
     * Checks a stress test.
     */
    public CheckStressTestEligibilityResponse checkStressTestEligibility(CheckStressTestEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckStressTestEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStressTest(request.getContext().getTenantId(), request.getStressTest());

        var results0 = new ArrayList<ExportPoliciesResponse>();
        for (var element : request.getStressTest().getLinesList()) {
            ExportPoliciesRequest itemRequest = ExportPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportPoliciesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getStressTest().getAudit().getUpdatedAt())
                        .putAllLabels(element.getAttributesMap())
                        .setOwnerId(element.getLineId())
                        .setRegion(request.getStressTest().getBillingAddress().getRegion())
                        .build())
                .build();
            var responses = insurancePolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportPolicies(itemRequest);
            responses.forEachRemaining(results0::add);
        }
        var reply0 = results0.isEmpty() ? ExportPoliciesResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = RecalculateStressTestsRequest.newBuilder()
                .setContext(request.getContext())
                .setStressTest(StressTest.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getStressTest().getAudit().getUpdatedAt())
                                .build())
                        .setBank(StressTestBankSettlement.newBuilder()
                                .setAccountHolder(switch (request.getStressTest().getSettlementCase()) {
                                    case BANK -> request.getStressTest().getBank().getAccountHolder();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(request.getStressTest().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply1 = riskStressTestCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateStressTests(outbound1);

        var outbound2 = CheckExposureEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setExposure(Exposure.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getStressTest().getCounterparty().getEmail())
                                .setName(request.getDetails().getContact().getName())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.risk.v1.ExposureValidationService");
        var selected2 = ExposureValidationServiceGrpc.newBlockingStub(channel2)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply2 = selected2.checkExposureEligibility(outbound2);

        var outbound3 = CheckRiskScenarioEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckRiskScenarioEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(reply2.getExposure().getContact().getPhone())
                                .build())
                        .build())
                .build();
        var reply3 = Stream.of(outbound3).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(riskRiskScenarioValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkRiskScenarioEligibility(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getStressTest().getStressTestId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckStressTestEligibility", request.getStressTest().getStressTestId());
        return CheckStressTestEligibilityResponse.newBuilder()
                .setOutcome(CheckStressTestEligibilityResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setStressTest(StressTest.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply3.getRiskScenario().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply2.getExposure().getHolder().getEmail())
                                .build())
                        .setNotes(request.getStressTest().getNotes())
                        .setStressTestId(request.getStressTestId())
                        .build())
                .build();
    }

    /**
     * Normalizes a stress test.
     */
    public NormalizeStressTestResponse normalizeStressTest(NormalizeStressTestRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeStressTest")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStressTest(request.getContext().getTenantId(), request.getStressTest());

        var outbound0 = ValidateRiskScenarioRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateRiskScenarioRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setName(request.getDetails().getContact().getName())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.risk.v1.RiskScenarioValidationService");
        var selected0 = RiskScenarioValidationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.validateRiskScenario(outbound0);

        var outbound1 = NormalizeRiskScenarioRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getRiskScenario().getAssignee().getDisplayName())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getStressTest().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        RpcPort<NormalizeRiskScenarioRequest, NormalizeRiskScenarioResponse> port1 = RpcPort.adapt(riskRiskScenarioValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::normalizeRiskScenario);
        var reply1 = port1.exchange(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getStressTest().getStressTestId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeStressTest", request.getStressTest().getStressTestId());
        return NormalizeStressTestResponse.newBuilder()
                .setOutcome(NormalizeStressTestResponse.Outcome.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .setStressTest(StressTest.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply1.getRiskScenario().getCustomer().getEmail())
                                .build())
                        .setNotes(request.getStressTest().getNotes())
                        .setStressTestId(request.getStressTestId())
                        .build())
                .build();
    }

    /**
     * Explains a stress test.
     */
    public ExplainStressTestDecisionResponse explainStressTestDecision(ExplainStressTestDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainStressTestDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStressTest(request.getContext().getTenantId(), request.getStressTest());

        var outbound0 = NormalizeAccessPolicyRequest.newBuilder()
                .setAccessPolicy(AccessPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getStressTest().getAudit().getCreatedBy())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(NormalizeAccessPolicyRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getStressTest().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> authorizationAccessPolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeAccessPolicy(value)).orElseThrow();

        var outbound1 = RevertJournalEntryRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RevertJournalEntryRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setJournalEntry(JournalEntry.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getStressTest().getAudit().getEtag())
                                .build())
                        .setBank(JournalEntryBankSettlement.newBuilder()
                                .setBic(switch (request.getStressTest().getSettlementCase()) {
                                    case BANK -> request.getStressTest().getBank().getBic();
                                    default -> "";
                                })
                                .build())
                        .addLines(JournalEntryLine.newBuilder()
                                .setTax(JournalEntryLineTax.newBuilder()
                                        .setAmount(reply0.getAccessPolicy().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply1 = ledgerJournalEntryHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).revertJournalEntry(outbound1);

        var outbound2 = JsonCodec.roundTrip(ExplainCollateralDecisionRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getStressTest().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build(), ExplainCollateralDecisionRequest.class);
        var reply2 = lendingCollateralValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainCollateralDecision(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getStressTest().getStressTestId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainStressTestDecision", request.getStressTest().getStressTestId());
        return ExplainStressTestDecisionResponse.newBuilder()
                .setOutcome(ExplainStressTestDecisionResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply1.getOutcome().getNextStepsList())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .setStressTest(StressTest.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getJournalEntry().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply2.getCollateral().getAddress().getLocality())
                                .build())
                        .setStressTestId(request.getStressTestId())
                        .setTotal(request.getStressTest().getTotal())
                        .build())
                .build();
    }

    /**
     * Scores a stress test.
     */
    public ScoreStressTestResponse scoreStressTest(ScoreStressTestRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreStressTest")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStressTest(request.getContext().getTenantId(), request.getStressTest());

        var results0 = new ArrayList<NormalizePremiumResponse>();
        for (var element : request.getStressTest().getLinesList()) {
            NormalizePremiumRequest itemRequest = NormalizePremiumRequest.newBuilder()
                .setContext(request.getContext())
                .setPremium(Premium.newBuilder()
                        .addLines(PremiumLine.newBuilder()
                                .setTax(PremiumLineTax.newBuilder()
                                        .addAllExemptions(element.getTax().getExemptionsList())
                                        .setRate(element.getTax().getRate())
                                        .build())
                                .build())
                        .build())
                .build();
            results0.add(insurancePremiumValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizePremium(itemRequest));
        }
        var reply0 = results0.isEmpty() ? NormalizePremiumResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = FindStatementsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindStatementsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getPremium().getAudit().getUpdatedAt())
                        .setRegion(request.getStressTest().getBillingAddress().getRegion())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.banking.v1.StatementQueryService");
        var selected1 = StatementQueryServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.findStatements(outbound1);

        var outbound2 = ExplainRiskScenarioDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setCustomer(CustomerRef.newBuilder()
                                .setCustomerId(request.getStressTest().getCounterparty().getCustomerId())
                                .build())
                        .build())
                .build();
        var reply2 = riskRiskScenarioValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainRiskScenarioDecision(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getStressTest().getStressTestId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreStressTest", request.getStressTest().getStressTestId());
        return ScoreStressTestResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply2.getResult().getMessage())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .setStressTest(StressTest.newBuilder()
                        .setIssuedAt(request.getStressTest().getIssuedAt())
                        .setStressTestId(request.getStressTestId())
                        .build())
                .build();
    }

    /**
     * Lints a stress test.
     */
    public LintStressTestsResponse lintStressTests(LintStressTestsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintStressTests")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStressTest(request.getContext().getTenantId(), request.getStressTest());

        var outbound0 = RpcSupport.unpack(Any.pack(ValidateStandingOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setStandingOrder(StandingOrder.newBuilder()
                        .addParticipants(StandingOrderParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getDetails().getContact().getEmail())
                                        .build())
                                .build())
                        .build())
                .build()), ValidateStandingOrderRequest.class);
        var reply0 = bankingStandingOrderValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateStandingOrder(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getStressTest().getStressTestId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintStressTests", request.getStressTest().getStressTestId());
        return LintStressTestsResponse.newBuilder()
                .setOutcome(LintStressTestsResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .setStressTest(StressTest.newBuilder()
                        .setStatus(request.getStressTest().getStatus())
                        .setStressTestId(request.getStressTestId())
                        .build())
                .build();
    }
}
