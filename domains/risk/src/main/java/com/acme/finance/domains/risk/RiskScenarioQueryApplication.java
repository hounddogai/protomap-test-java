package com.acme.finance.domains.risk;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ClaimQueryServiceGrpc;
import com.acme.insurance.v1.ComparePolicyOptionsRequest;
import com.acme.insurance.v1.ComparePolicyOptionsResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.insurance.v1.PolicyCardFunding;
import com.acme.insurance.v1.ResolveClaimsRequest;
import com.acme.insurance.v1.ResolveClaimsResponse;
import com.acme.risk.v1.BrowseRiskScenariosRequest;
import com.acme.risk.v1.BrowseRiskScenariosResponse;
import com.acme.risk.v1.CountRiskScenariosRequest;
import com.acme.risk.v1.CountRiskScenariosResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureCalculationServiceGrpc;
import com.acme.risk.v1.LookupRiskScenarioRequest;
import com.acme.risk.v1.LookupRiskScenarioResponse;
import com.acme.risk.v1.PeekRiskScenarioRequest;
import com.acme.risk.v1.PeekRiskScenarioResponse;
import com.acme.risk.v1.RecalculateExposuresRequest;
import com.acme.risk.v1.RecalculateExposuresResponse;
import com.acme.risk.v1.ResolveRiskScenariosRequest;
import com.acme.risk.v1.ResolveRiskScenariosResponse;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.SimulateStressTestRequest;
import com.acme.risk.v1.SimulateStressTestResponse;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestCalculationServiceGrpc;
import com.acme.risk.v1.StressTestLine;
import com.acme.risk.v1.SummarizeRiskScenariosRequest;
import com.acme.risk.v1.SummarizeRiskScenariosResponse;
import com.acme.venues.v1.CreateVenueRequest;
import com.acme.venues.v1.CreateVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueServiceGrpc;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Answers read-only questions about risk scenarios, for screens and reports. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class RiskScenarioQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(RiskScenarioQueryApplication.class);

    private final AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final ClaimQueryServiceGrpc.ClaimQueryServiceBlockingStub insuranceClaimQueryServiceBlocking;

    private final PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking;

    private final RiskScenarioQueryRequestMapper mapper;

    private final RiskRepository repository;

    private final ExposureCalculationServiceGrpc.ExposureCalculationServiceBlockingStub riskExposureCalculationServiceBlocking;

    private final StressTestCalculationServiceGrpc.StressTestCalculationServiceBlockingStub riskStressTestCalculationServiceBlocking;

    private final VenueServiceGrpc.VenueServiceBlockingStub venuesVenueServiceBlocking;

    public RiskScenarioQueryApplication(
            AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            ClaimQueryServiceGrpc.ClaimQueryServiceBlockingStub insuranceClaimQueryServiceBlocking,
            PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking,
            RiskScenarioQueryRequestMapper mapper,
            RiskRepository repository,
            ExposureCalculationServiceGrpc.ExposureCalculationServiceBlockingStub riskExposureCalculationServiceBlocking,
            StressTestCalculationServiceGrpc.StressTestCalculationServiceBlockingStub riskStressTestCalculationServiceBlocking,
            VenueServiceGrpc.VenueServiceBlockingStub venuesVenueServiceBlocking) {
        this.auditAuditEventServiceAsync = auditAuditEventServiceAsync;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.insuranceClaimQueryServiceBlocking = insuranceClaimQueryServiceBlocking;
        this.insurancePolicyCalculationServiceBlocking = insurancePolicyCalculationServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.riskExposureCalculationServiceBlocking = riskExposureCalculationServiceBlocking;
        this.riskStressTestCalculationServiceBlocking = riskStressTestCalculationServiceBlocking;
        this.venuesVenueServiceBlocking = venuesVenueServiceBlocking;
    }

    /**
     * Returns a risk scenario.
     */
    public LookupRiskScenarioResponse lookupRiskScenario(LookupRiskScenarioRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupRiskScenario")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RiskScenario", request.getRiskScenarioId(), "LookupRiskScenario");

        var outbound0 = BatchCreateAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateAuditEventsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector0 = new StreamCollector<>(BatchCreateAuditEventsResponse.getDefaultInstance());
        var input0 = auditAuditEventServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateAuditEvents(collector0);
        input0.onNext(outbound0);
        input0.onCompleted();
        var reply0 = collector0.await();

        var outbound1 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setBlock(RetentionPolicyBlockAction.newBuilder()
                                .setNotifyEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector1 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input1 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();
        return LookupRiskScenarioResponse.newBuilder()
                .setRiskScenario(RiskScenario.newBuilder()
                        .setRiskScenarioId(request.getRiskScenarioId())
                        .build())
                .build();
    }

    /**
     * Lists risk scenarios a page at a time.
     */
    public BrowseRiskScenariosResponse browseRiskScenarios(BrowseRiskScenariosRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowseRiskScenarios")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RiskScenario", request.getContext().getTraceId(), "BrowseRiskScenarios");

        var outbound0 = CreateVenueRequest.newBuilder()
                .setContext(request.getContext())
                .setVenue(Venue.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        RpcPort<CreateVenueRequest, CreateVenueResponse> port0 = RpcPort.adapt(venuesVenueServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::createVenue);
        var reply0 = port0.exchange(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowseRiskScenarios", request.getPage().getPageToken());
        return BrowseRiskScenariosResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Counts risk scenarios by status.
     */
    public CountRiskScenariosResponse countRiskScenarios(CountRiskScenariosRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountRiskScenarios")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RiskScenario", request.getContext().getTraceId(), "CountRiskScenarios");

        var outbound0 = (ResolveClaimsRequest.newBuilder()
                .setContext(request.getContext())
                .build()).toBuilder().build();
        var reply0 = insuranceClaimQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveClaims(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountRiskScenarios", request.getPage().getPageToken());
        return CountRiskScenariosResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns several risk scenarios at once.
     */
    public ResolveRiskScenariosResponse resolveRiskScenarios(ResolveRiskScenariosRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveRiskScenarios")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RiskScenario", request.getContext().getTraceId(), "ResolveRiskScenarios");

        var outbound0 = SimulateStressTestRequest.newBuilder()
                .setContext(request.getContext())
                .setStressTest(StressTest.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .putAllMetadata(request.getContext().getBaggageMap())
                        .build())
                .build();
        RpcPort<SimulateStressTestRequest, SimulateStressTestResponse> port0 = RpcPort.adapt(riskStressTestCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::simulateStressTest);
        var reply0 = port0.exchange(outbound0);
        return ResolveRiskScenariosResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .addRiskScenarios(RiskScenario.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply0.getStressTest().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
    }

    /**
     * Summarizes a risk scenario.
     */
    public SummarizeRiskScenariosResponse summarizeRiskScenarios(SummarizeRiskScenariosRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SummarizeRiskScenarios")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RiskScenario", request.getRiskScenarioId(), "SummarizeRiskScenarios");

        var outbound0 = mapper.summarizeRiskScenariosStage0(request);
        var reply0 = riskStressTestCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateStressTest(outbound0);

        var outbound1 = ComparePolicyOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ComparePolicyOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getStressTest().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setCard(PolicyCardFunding.newBuilder()
                                .setLast4(switch (reply0.getStressTest().getSettlementCase()) {
                                    case CARD -> reply0.getStressTest().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).comparePolicyOptions(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getRiskScenarioId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SummarizeRiskScenarios", request.getRiskScenarioId());
        return SummarizeRiskScenariosResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a risk scenario.
     */
    public PeekRiskScenarioResponse peekRiskScenario(PeekRiskScenarioRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PeekRiskScenario")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RiskScenario", request.getRiskScenarioId(), "PeekRiskScenario");

        var outbound0 = RecalculateExposuresRequest.newBuilder()
                .setContext(request.getContext())
                .setExposure(Exposure.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> riskExposureCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateExposures(value)).orElseThrow();
        return PeekRiskScenarioResponse.newBuilder()
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getExposure().getAudit().getUpdatedAt())
                                .build())
                        .setRiskScenarioId(request.getRiskScenarioId())
                        .build())
                .build();
    }
}
