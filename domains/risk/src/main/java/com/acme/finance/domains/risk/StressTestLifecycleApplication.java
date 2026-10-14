package com.acme.finance.domains.risk;

import com.acme.audit.v1.AuditEventHistoryServiceGrpc;
import com.acme.audit.v1.ListAuditEventVersionsRequest;
import com.acme.audit.v1.ListAuditEventVersionsResponse;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.HouseholdQueryServiceGrpc;
import com.acme.customers.v1.SummarizeHouseholdsRequest;
import com.acme.customers.v1.SummarizeHouseholdsResponse;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.performance.v1.ListReviewsRequest;
import com.acme.performance.v1.ListReviewsResponse;
import com.acme.performance.v1.ReviewServiceGrpc;
import com.acme.performance.v1.WatchReviewsRequest;
import com.acme.performance.v1.WatchReviewsResponse;
import com.acme.prescriptions.v1.PrescriptionServiceGrpc;
import com.acme.prescriptions.v1.WatchPrescriptionsRequest;
import com.acme.prescriptions.v1.WatchPrescriptionsResponse;
import com.acme.risk.v1.ActivateStressTestRequest;
import com.acme.risk.v1.ActivateStressTestResponse;
import com.acme.risk.v1.BrowseRiskScenariosRequest;
import com.acme.risk.v1.BrowseRiskScenariosResponse;
import com.acme.risk.v1.CalculateStressTestRequest;
import com.acme.risk.v1.CalculateStressTestResponse;
import com.acme.risk.v1.ExpireStressTestRequest;
import com.acme.risk.v1.ExpireStressTestResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureCalculationServiceGrpc;
import com.acme.risk.v1.ListRiskRegistersRequest;
import com.acme.risk.v1.ListRiskRegistersResponse;
import com.acme.risk.v1.ReinstateStressTestRequest;
import com.acme.risk.v1.ReinstateStressTestResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterServiceGrpc;
import com.acme.risk.v1.RiskScenarioQueryServiceGrpc;
import com.acme.risk.v1.ScheduleStressTestExpiryRequest;
import com.acme.risk.v1.ScheduleStressTestExpiryResponse;
import com.acme.risk.v1.SimulateExposureRequest;
import com.acme.risk.v1.SimulateExposureResponse;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestCalculationServiceGrpc;
import com.acme.risk.v1.SuspendStressTestRequest;
import com.acme.risk.v1.SuspendStressTestResponse;
import com.acme.risk.v1.TerminateStressTestRequest;
import com.acme.risk.v1.TerminateStressTestResponse;
import com.acme.risk.v1.UpdateRiskRegisterRequest;
import com.acme.risk.v1.UpdateRiskRegisterResponse;
import com.acme.suppliers.v1.SuggestSupplierContractsRequest;
import com.acme.suppliers.v1.SuggestSupplierContractsResponse;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.suppliers.v1.ValidateSupplierRatingRequest;
import com.acme.suppliers.v1.ValidateSupplierRatingResponse;
import com.acme.venues.v1.SuggestVenuesRequest;
import com.acme.venues.v1.SuggestVenuesResponse;
import com.acme.venues.v1.VenueQueryServiceGrpc;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Moves a stress test between the states of its lifecycle. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class StressTestLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(StressTestLifecycleApplication.class);

    private final AuditEventHistoryServiceGrpc.AuditEventHistoryServiceBlockingStub auditAuditEventHistoryServiceBlocking;

    private final HouseholdQueryServiceGrpc.HouseholdQueryServiceBlockingStub customersHouseholdQueryServiceBlocking;

    private final ApplicationEventPublisher events;

    private final StressTestLifecycleRequestMapper mapper;

    private final ReviewServiceGrpc.ReviewServiceBlockingStub performanceReviewServiceBlocking;

    private final PrescriptionServiceGrpc.PrescriptionServiceBlockingStub prescriptionsPrescriptionServiceBlocking;

    private final RiskRepository repository;

    private final ExposureCalculationServiceGrpc.ExposureCalculationServiceBlockingStub riskExposureCalculationServiceBlocking;

    private final RiskRegisterServiceGrpc.RiskRegisterServiceBlockingStub riskRiskRegisterServiceBlocking;

    private final RiskScenarioQueryServiceGrpc.RiskScenarioQueryServiceBlockingStub riskRiskScenarioQueryServiceBlocking;

    private final StressTestCalculationServiceGrpc.StressTestCalculationServiceBlockingStub riskStressTestCalculationServiceBlocking;

    private final Executor rpcExecutor;

    private final SupplierContractQueryServiceGrpc.SupplierContractQueryServiceBlockingStub suppliersSupplierContractQueryServiceBlocking;

    private final SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking;

    private final VenueQueryServiceGrpc.VenueQueryServiceBlockingStub venuesVenueQueryServiceBlocking;

    public StressTestLifecycleApplication(
            AuditEventHistoryServiceGrpc.AuditEventHistoryServiceBlockingStub auditAuditEventHistoryServiceBlocking,
            HouseholdQueryServiceGrpc.HouseholdQueryServiceBlockingStub customersHouseholdQueryServiceBlocking,
            ApplicationEventPublisher events,
            StressTestLifecycleRequestMapper mapper,
            ReviewServiceGrpc.ReviewServiceBlockingStub performanceReviewServiceBlocking,
            PrescriptionServiceGrpc.PrescriptionServiceBlockingStub prescriptionsPrescriptionServiceBlocking,
            RiskRepository repository,
            ExposureCalculationServiceGrpc.ExposureCalculationServiceBlockingStub riskExposureCalculationServiceBlocking,
            RiskRegisterServiceGrpc.RiskRegisterServiceBlockingStub riskRiskRegisterServiceBlocking,
            RiskScenarioQueryServiceGrpc.RiskScenarioQueryServiceBlockingStub riskRiskScenarioQueryServiceBlocking,
            StressTestCalculationServiceGrpc.StressTestCalculationServiceBlockingStub riskStressTestCalculationServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierContractQueryServiceGrpc.SupplierContractQueryServiceBlockingStub suppliersSupplierContractQueryServiceBlocking,
            SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking,
            VenueQueryServiceGrpc.VenueQueryServiceBlockingStub venuesVenueQueryServiceBlocking) {
        this.auditAuditEventHistoryServiceBlocking = auditAuditEventHistoryServiceBlocking;
        this.customersHouseholdQueryServiceBlocking = customersHouseholdQueryServiceBlocking;
        this.events = events;
        this.mapper = mapper;
        this.performanceReviewServiceBlocking = performanceReviewServiceBlocking;
        this.prescriptionsPrescriptionServiceBlocking = prescriptionsPrescriptionServiceBlocking;
        this.repository = repository;
        this.riskExposureCalculationServiceBlocking = riskExposureCalculationServiceBlocking;
        this.riskRiskRegisterServiceBlocking = riskRiskRegisterServiceBlocking;
        this.riskRiskScenarioQueryServiceBlocking = riskRiskScenarioQueryServiceBlocking;
        this.riskStressTestCalculationServiceBlocking = riskStressTestCalculationServiceBlocking;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierContractQueryServiceBlocking = suppliersSupplierContractQueryServiceBlocking;
        this.suppliersSupplierRatingValidationServiceBlocking = suppliersSupplierRatingValidationServiceBlocking;
        this.venuesVenueQueryServiceBlocking = venuesVenueQueryServiceBlocking;
    }

    public record ActivateStressTestStage1Requested(UpdateRiskRegisterRequest request, CompletableFuture<UpdateRiskRegisterResponse> completion, io.grpc.Context context) {}

    /**
     * Activates a stress test.
     */
    public ActivateStressTestResponse activateStressTest(ActivateStressTestRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivateStressTest")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "StressTest", request.getStressTestId(), "ActivateStressTest");

        var outbound0 = WatchReviewsRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getNewOwner().getEmail())
                        .setPhone(request.getDetails().getNotifyContact().getPhone())
                        .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                        .build())
                .build();
        var stream0 = performanceReviewServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchReviews(outbound0);
        var reply0 = WatchReviewsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = UpdateRiskRegisterRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                                .build())
                                        .build())
                                .setManager(UserRef.newBuilder()
                                        .setEmail(request.getDetails().getNotifyContact().getEmail())
                                        .setUserId(request.getDetails().getNewOwner().getUserId())
                                        .build())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<UpdateRiskRegisterResponse>();
        events.publishEvent(new ActivateStressTestStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivateStressTest", request.getDetails().getReasonCode());
        return ActivateStressTestResponse.newBuilder()
                .setOutcome(ActivateStressTestResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .setStressTest(StressTest.newBuilder()
                        .setStressTestId(request.getStressTestId())
                        .build())
                .build();
    }

    /**
     * Suspends a stress test.
     */
    public SuspendStressTestResponse suspendStressTest(SuspendStressTestRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendStressTest")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "StressTest", request.getStressTestId(), "SuspendStressTest");

        var outbound0 = CalculateStressTestRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setReason(request.getReason())
                .setStressTest(StressTest.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = riskStressTestCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateStressTest(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendStressTest", request.getDetails().getReasonCode());
        return SuspendStressTestResponse.newBuilder()
                .setOutcome(SuspendStressTestResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setStressTest(StressTest.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setCustomerId(reply0.getStressTest().getCounterparty().getCustomerId())
                                .build())
                        .setStressTestId(request.getStressTestId())
                        .build())
                .build();
    }

    /**
     * Reinstates a stress test.
     */
    public ReinstateStressTestResponse reinstateStressTest(ReinstateStressTestRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReinstateStressTest")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "StressTest", request.getStressTestId(), "ReinstateStressTest");

        var outbound0 = mapper.reinstateStressTestStage0(request);
        var reply0 = riskRiskRegisterServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listRiskRegisters(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReinstateStressTest", request.getDetails().getReasonCode());
        return ReinstateStressTestResponse.newBuilder()
                .setOutcome(ReinstateStressTestResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .setStressTest(StressTest.newBuilder()
                        .setStressTestId(request.getStressTestId())
                        .build())
                .build();
    }

    /**
     * Terminates a stress test.
     */
    public TerminateStressTestResponse terminateStressTest(TerminateStressTestRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TerminateStressTest")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "StressTest", request.getStressTestId(), "TerminateStressTest");

        var outbound0 = RpcSupport.unpack(Any.pack(SuggestSupplierContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestSupplierContractsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build()), SuggestSupplierContractsRequest.class);
        var reply0 = suppliersSupplierContractQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestSupplierContracts(outbound0);

        var outbound1 = ListAuditEventVersionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAuditEventVersionsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setOwnerId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(auditAuditEventHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listAuditEventVersions(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TerminateStressTest", request.getDetails().getReasonCode());
        return TerminateStressTestResponse.newBuilder()
                .setOutcome(TerminateStressTestResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .setStressTest(StressTest.newBuilder()
                        .setStressTestId(request.getStressTestId())
                        .build())
                .build();
    }

    /**
     * Expires a stress test.
     */
    public ExpireStressTestResponse expireStressTest(ExpireStressTestRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExpireStressTest")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "StressTest", request.getStressTestId(), "ExpireStressTest");

        var outbound0 = ListReviewsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListReviewsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .setOwnerId(request.getStressTestId())
                        .build())
                .build();
        Function<ListReviewsRequest, ListReviewsResponse> operation0 = performanceReviewServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::listReviews;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = SimulateExposureRequest.newBuilder()
                .setContext(request.getContext())
                .setExposure(Exposure.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .setHolder(SupplierRef.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.risk.v1.ExposureCalculationService/SimulateExposure", outbound1);
        var reply1 = riskExposureCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateExposure(command1.payload());

        var outbound2 = WatchPrescriptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getNewOwner().getEmail())
                        .setName(reply1.getExposure().getContact().getName())
                        .setPhone(request.getDetails().getNotifyContact().getPhone())
                        .setPreferredChannel(reply1.getExposure().getContact().getPreferredChannel())
                        .build())
                .build();
        var stream2 = prescriptionsPrescriptionServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchPrescriptions(outbound2);
        var reply2 = WatchPrescriptionsResponse.getDefaultInstance();
        while (stream2.hasNext()) {
            reply2 = stream2.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExpireStressTest", request.getDetails().getReasonCode());
        return ExpireStressTestResponse.newBuilder()
                .setOutcome(ExpireStressTestResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setStressTest(StressTest.newBuilder()
                        .setStressTestId(request.getStressTestId())
                        .build())
                .build();
    }

    /**
     * Schedules a stress test.
     */
    public ScheduleStressTestExpiryResponse scheduleStressTestExpiry(ScheduleStressTestExpiryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleStressTestExpiry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStressTest(request.getContext().getTenantId(), request.getStressTest());

        var outbound0 = ValidateSupplierRatingRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getStressTest().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> suppliersSupplierRatingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateSupplierRating(outbound0), rpcExecutor);

        var outbound1 = SummarizeHouseholdsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SummarizeHouseholdsRequest.Details.newBuilder()
                        .putAllFilters(request.getStressTest().getMetadataMap())
                        .build())
                .setReason(request.getReason())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> customersHouseholdQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeHouseholds(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = SuggestVenuesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestVenuesRequest.Filter.newBuilder()
                        .setRegion(reply0.getSupplierRating().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(venuesVenueQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestVenues(value)))
                .collect(Collectors.toList()).getFirst();

        var results3 = new ArrayList<BrowseRiskScenariosResponse>();
        for (var element : request.getStressTest().getLinesList()) {
            BrowseRiskScenariosRequest itemRequest = BrowseRiskScenariosRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseRiskScenariosRequest.Filter.newBuilder()
                        .putAllLabels(element.getAttributesMap())
                        .setOwnerId(element.getLineId())
                        .setRegion(reply0.getSupplierRating().getBillingAddress().getRegion())
                        .build())
                .build();
            results3.add(riskRiskScenarioQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseRiskScenarios(itemRequest));
        }
        var reply3 = results3.isEmpty() ? BrowseRiskScenariosResponse.getDefaultInstance()
                : results3.getLast();
        log.atDebug().addKeyValue("auditValue", request.getStressTest().getStressTestId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleStressTestExpiry", request.getStressTest().getStressTestId());
        return ScheduleStressTestExpiryResponse.newBuilder()
                .setOutcome(ScheduleStressTestExpiryResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .setStressTest(StressTest.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setCustomerId(reply0.getSupplierRating().getCounterparty().getCustomerId())
                                .build())
                        .putAllMetadata(request.getStressTest().getMetadataMap())
                        .setStressTestId(request.getStressTestId())
                        .build())
                .build();
    }
}
