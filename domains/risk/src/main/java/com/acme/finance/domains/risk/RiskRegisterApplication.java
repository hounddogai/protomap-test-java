package com.acme.finance.domains.risk;

import com.acme.collections.v1.CollectionCaseQueryServiceGrpc;
import com.acme.collections.v1.SummarizeCollectionCasesRequest;
import com.acme.collections.v1.SummarizeCollectionCasesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CompareUnderwritingOptionsRequest;
import com.acme.insurance.v1.CompareUnderwritingOptionsResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingCalculationServiceGrpc;
import com.acme.insurance.v1.UnderwritingComment;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralValidationServiceGrpc;
import com.acme.lending.v1.ExplainLoanApplicationDecisionRequest;
import com.acme.lending.v1.ExplainLoanApplicationDecisionResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationValidationServiceGrpc;
import com.acme.lending.v1.NormalizeLoanApplicationRequest;
import com.acme.lending.v1.NormalizeLoanApplicationResponse;
import com.acme.lending.v1.PreviewCollateralRequest;
import com.acme.lending.v1.PreviewCollateralResponse;
import com.acme.notifications.v1.CreateNotificationRequest;
import com.acme.notifications.v1.CreateNotificationResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.risk.v1.BatchCreateRiskRegistersRequest;
import com.acme.risk.v1.BatchCreateRiskRegistersResponse;
import com.acme.risk.v1.BatchGetRiskRegistersRequest;
import com.acme.risk.v1.BatchGetRiskRegistersResponse;
import com.acme.risk.v1.CheckRiskRegisterEligibilityRequest;
import com.acme.risk.v1.CheckRiskRegisterEligibilityResponse;
import com.acme.risk.v1.CloneRiskRegisterRequest;
import com.acme.risk.v1.CloneRiskRegisterResponse;
import com.acme.risk.v1.CountRiskScenariosRequest;
import com.acme.risk.v1.CountRiskScenariosResponse;
import com.acme.risk.v1.CreateRiskRegisterRequest;
import com.acme.risk.v1.CreateRiskRegisterResponse;
import com.acme.risk.v1.DeleteRiskRegisterRequest;
import com.acme.risk.v1.DeleteRiskRegisterResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureValidationServiceGrpc;
import com.acme.risk.v1.GetRiskRegisterRequest;
import com.acme.risk.v1.GetRiskRegisterResponse;
import com.acme.risk.v1.GetRiskRegisterRulesRequest;
import com.acme.risk.v1.GetRiskRegisterRulesResponse;
import com.acme.risk.v1.LintExposuresRequest;
import com.acme.risk.v1.LintExposuresResponse;
import com.acme.risk.v1.ListRiskRegistersRequest;
import com.acme.risk.v1.ListRiskRegistersResponse;
import com.acme.risk.v1.ResolveRiskRegistersRequest;
import com.acme.risk.v1.ResolveRiskRegistersResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterValidationServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioQueryServiceGrpc;
import com.acme.risk.v1.RiskScenarioValidationServiceGrpc;
import com.acme.risk.v1.ScoreExposureRequest;
import com.acme.risk.v1.ScoreExposureResponse;
import com.acme.risk.v1.SearchRiskRegistersRequest;
import com.acme.risk.v1.SearchRiskRegistersResponse;
import com.acme.risk.v1.SummarizeRiskScenariosRequest;
import com.acme.risk.v1.SummarizeRiskScenariosResponse;
import com.acme.risk.v1.UpdateRiskRegisterRequest;
import com.acme.risk.v1.UpdateRiskRegisterResponse;
import com.acme.risk.v1.ValidateRiskScenarioRequest;
import com.acme.risk.v1.ValidateRiskScenarioResponse;
import com.acme.risk.v1.WatchRiskRegistersRequest;
import com.acme.risk.v1.WatchRiskRegistersResponse;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.venues.v1.LintVenuesRequest;
import com.acme.venues.v1.LintVenuesResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueValidationServiceGrpc;
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
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Manages risk registers: creates, reads, changes, and lists them. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class RiskRegisterApplication {
    private static final Logger log = LoggerFactory.getLogger(RiskRegisterApplication.class);

    private final ChannelRegistry channels;

    private final CollectionCaseQueryServiceGrpc.CollectionCaseQueryServiceBlockingStub collectionsCollectionCaseQueryServiceBlocking;

    private final ApplicationEventPublisher events;

    private final UnderwritingCalculationServiceGrpc.UnderwritingCalculationServiceBlockingStub insuranceUnderwritingCalculationServiceBlocking;

    private final CollateralValidationServiceGrpc.CollateralValidationServiceBlockingStub lendingCollateralValidationServiceBlocking;

    private final LoanApplicationValidationServiceGrpc.LoanApplicationValidationServiceBlockingStub lendingLoanApplicationValidationServiceBlocking;

    private final RiskRegisterRequestMapper mapper;

    private final NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking;

    private final RiskRepository repository;

    private final ExposureValidationServiceGrpc.ExposureValidationServiceBlockingStub riskExposureValidationServiceBlocking;

    private final RiskRegisterValidationServiceGrpc.RiskRegisterValidationServiceBlockingStub riskRiskRegisterValidationServiceBlocking;

    private final RiskRegisterValidationServiceGrpc.RiskRegisterValidationServiceFutureStub riskRiskRegisterValidationServiceFuture;

    private final RiskScenarioQueryServiceGrpc.RiskScenarioQueryServiceBlockingStub riskRiskScenarioQueryServiceBlocking;

    private final RiskScenarioValidationServiceGrpc.RiskScenarioValidationServiceBlockingStub riskRiskScenarioValidationServiceBlocking;

    private final Executor rpcExecutor;

    private final VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking;

    public RiskRegisterApplication(
            ChannelRegistry channels,
            CollectionCaseQueryServiceGrpc.CollectionCaseQueryServiceBlockingStub collectionsCollectionCaseQueryServiceBlocking,
            ApplicationEventPublisher events,
            UnderwritingCalculationServiceGrpc.UnderwritingCalculationServiceBlockingStub insuranceUnderwritingCalculationServiceBlocking,
            CollateralValidationServiceGrpc.CollateralValidationServiceBlockingStub lendingCollateralValidationServiceBlocking,
            LoanApplicationValidationServiceGrpc.LoanApplicationValidationServiceBlockingStub lendingLoanApplicationValidationServiceBlocking,
            RiskRegisterRequestMapper mapper,
            NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking,
            RiskRepository repository,
            ExposureValidationServiceGrpc.ExposureValidationServiceBlockingStub riskExposureValidationServiceBlocking,
            RiskRegisterValidationServiceGrpc.RiskRegisterValidationServiceBlockingStub riskRiskRegisterValidationServiceBlocking,
            RiskRegisterValidationServiceGrpc.RiskRegisterValidationServiceFutureStub riskRiskRegisterValidationServiceFuture,
            RiskScenarioQueryServiceGrpc.RiskScenarioQueryServiceBlockingStub riskRiskScenarioQueryServiceBlocking,
            RiskScenarioValidationServiceGrpc.RiskScenarioValidationServiceBlockingStub riskRiskScenarioValidationServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking) {
        this.channels = channels;
        this.collectionsCollectionCaseQueryServiceBlocking = collectionsCollectionCaseQueryServiceBlocking;
        this.events = events;
        this.insuranceUnderwritingCalculationServiceBlocking = insuranceUnderwritingCalculationServiceBlocking;
        this.lendingCollateralValidationServiceBlocking = lendingCollateralValidationServiceBlocking;
        this.lendingLoanApplicationValidationServiceBlocking = lendingLoanApplicationValidationServiceBlocking;
        this.mapper = mapper;
        this.notificationsNotificationServiceBlocking = notificationsNotificationServiceBlocking;
        this.repository = repository;
        this.riskExposureValidationServiceBlocking = riskExposureValidationServiceBlocking;
        this.riskRiskRegisterValidationServiceBlocking = riskRiskRegisterValidationServiceBlocking;
        this.riskRiskRegisterValidationServiceFuture = riskRiskRegisterValidationServiceFuture;
        this.riskRiskScenarioQueryServiceBlocking = riskRiskScenarioQueryServiceBlocking;
        this.riskRiskScenarioValidationServiceBlocking = riskRiskScenarioValidationServiceBlocking;
        this.rpcExecutor = rpcExecutor;
        this.venuesVenueValidationServiceBlocking = venuesVenueValidationServiceBlocking;
    }

    public record ListRiskRegistersStage0Requested(ExplainLoanApplicationDecisionRequest request, CompletableFuture<ExplainLoanApplicationDecisionResponse> completion, io.grpc.Context context) {}

    /**
     * Creates a risk register.
     */
    public CreateRiskRegisterResponse createRiskRegister(CreateRiskRegisterRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CreateRiskRegister")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskRegister(request.getContext().getTenantId(), request.getRiskRegister());

        var outbound0 = SummarizeCollectionCasesRequest.newBuilder()
                .putAllAnnotations(request.getContext().getBaggageMap())
                .setContext(request.getContext())
                .setDetails(SummarizeCollectionCasesRequest.Details.newBuilder()
                        .putAllFilters(request.getRiskRegister().getAttributesMap())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> collectionsCollectionCaseQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeCollectionCases(outbound0), rpcExecutor);

        var outbound1 = ScoreExposureRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScoreExposureRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getRiskRegister().getLocation().getCountryCode())
                                .setRecipient(request.getRiskRegister().getLocation().getRecipient())
                                .build())
                        .build())
                .setExposure(Exposure.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getRiskRegister().getAudit().getUpdatedAt())
                                .setUpdatedBy(request.getRiskRegister().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> riskExposureValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreExposure(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getRiskRegister().getRiskRegisterId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CreateRiskRegister", request.getRiskRegister().getRiskRegisterId());
        return CreateRiskRegisterResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .setRiskRegister(RiskRegister.newBuilder()
                        .setName(request.getRiskRegister().getName())
                        .setStatus(request.getRiskRegister().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a risk register.
     */
    public GetRiskRegisterResponse getRiskRegister(GetRiskRegisterRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetRiskRegister")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RiskRegister", request.getRiskRegisterId(), "GetRiskRegister");

        var outbound0 = SummarizeRiskScenariosRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SummarizeRiskScenariosRequest.Details.newBuilder()
                        .putAllFilters(request.getContext().getBaggageMap())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> riskRiskScenarioQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeRiskScenarios(outbound0), rpcExecutor);

        var outbound1 = CreateNotificationRequest.newBuilder()
                .setContext(request.getContext())
                .setNotification(Notification.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> notificationsNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createNotification(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        return GetRiskRegisterResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .setRiskRegister(RiskRegister.newBuilder()
                        .setManager(UserRef.newBuilder()
                                .setDisplayName(reply1.getNotification().getCustomer().getDisplayName())
                                .setUserId(reply1.getNotification().getAssignee().getUserId())
                                .build())
                        .setRiskRegisterId(request.getRiskRegisterId())
                        .build())
                .build();
    }

    /**
     * Changes a risk register.
     */
    public UpdateRiskRegisterResponse updateRiskRegister(UpdateRiskRegisterRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "UpdateRiskRegister")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskRegister(request.getContext().getTenantId(), request.getRiskRegister());

        var outbound0 = (CountRiskScenariosRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountRiskScenariosRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getRiskRegister().getAudit().getUpdatedAt())
                        .setRegion(request.getRiskRegister().getLocation().getRegion())
                        .build())
                .build()).toBuilder().build();
        var reply0 = riskRiskScenarioQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countRiskScenarios(outbound0);

        var outbound1 = BeanCopies.copy(LintExposuresRequest.newBuilder()
                .setContext(request.getContext())
                .setExposure(Exposure.newBuilder()
                        .setHolder(SupplierRef.newBuilder()
                                .setDisplayName(request.getRiskRegister().getManager().getDisplayName())
                                .build())
                        .build())
                .build(), LintExposuresRequest.class);
        var reply1 = riskExposureValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintExposures(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getRiskRegister().getRiskRegisterId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "UpdateRiskRegister", request.getRiskRegister().getRiskRegisterId());
        return UpdateRiskRegisterResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .setRiskRegister(RiskRegister.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getExposure().getAudit().getCreatedAt())
                                .build())
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setUpdatedBy(reply1.getExposure().getAudit().getUpdatedBy())
                                                .build())
                                        .build())
                                .build())
                        .setManager(request.getRiskRegister().getManager())
                        .setStatus(request.getRiskRegister().getStatus())
                        .build())
                .build();
    }

    /**
     * Deletes a risk register.
     */
    public DeleteRiskRegisterResponse deleteRiskRegister(DeleteRiskRegisterRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DeleteRiskRegister")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RiskRegister", request.getRiskRegisterId(), "DeleteRiskRegister");

        var outbound0 = CheckRiskRegisterEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setReason(request.getReason())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .setAudit(AuditInfo.newBuilder()
                                        .setEtag(request.getEtag())
                                        .build())
                                .addChildren(RiskRegister.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setUserId(request.getContext().getActor().getUserId())
                                                .build())
                                        .setRiskRegisterId(request.getRiskRegisterId())
                                        .build())
                                .build())
                        .build())
                .build();
        var pending0 = riskRiskRegisterValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).checkRiskRegisterEligibility(outbound0);
        var reply0 = RpcSupport.await(pending0);
        return DeleteRiskRegisterResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists risk registers a page at a time.
     */
    public ListRiskRegistersResponse listRiskRegisters(ListRiskRegistersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListRiskRegisters")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RiskRegister", request.getContext().getTraceId(), "ListRiskRegisters");

        var outbound0 = ExplainLoanApplicationDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<ExplainLoanApplicationDecisionResponse>();
        events.publishEvent(new ListRiskRegistersStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListRiskRegisters", request.getPage().getPageToken());
        return ListRiskRegistersResponse.newBuilder()
                .addRiskRegisters(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .setLocation(PostalAddress.newBuilder()
                                        .setPostalCode(reply0.getLoanApplication().getBillingAddress().getPostalCode())
                                        .build())
                                .build())
                        .build())
                .build();
    }

    /**
     * Returns several risk registers at once.
     */
    public BatchGetRiskRegistersResponse batchGetRiskRegisters(BatchGetRiskRegistersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BatchGetRiskRegisters")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RiskRegister", request.getContext().getTraceId(), "BatchGetRiskRegisters");
        return BatchGetRiskRegistersResponse.getDefaultInstance();
    }

    /**
     * Lists risk registers a page at a time.
     */
    public SearchRiskRegistersResponse searchRiskRegisters(SearchRiskRegistersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SearchRiskRegisters")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RiskRegister", request.getContext().getTraceId(), "SearchRiskRegisters");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SearchRiskRegisters", request.getPage().getPageToken());
        return SearchRiskRegistersResponse.getDefaultInstance();
    }

    /**
     * Streams the changes of risk registers as they happen.
     */
    public WatchRiskRegistersResponse watchRiskRegisters(WatchRiskRegistersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "WatchRiskRegisters")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RiskRegister", request.getContext().getTraceId(), "WatchRiskRegisters");

        var outbound0 = JsonCodec.roundTrip(NormalizeLoanApplicationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeLoanApplicationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getSubscriber().getName())
                                .build())
                        .build())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .build())
                        .build())
                .build(), NormalizeLoanApplicationRequest.class);
        var reply0 = lendingLoanApplicationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeLoanApplication(outbound0);

        var outbound1 = PreviewCollateralRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getLoanApplication().getBillingAddress().getCountryCode())
                                .addAllLines(reply0.getLoanApplication().getBillingAddress().getLinesList())
                                .setLocality(reply0.getLoanApplication().getBillingAddress().getLocality())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PreviewCollateralRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getLoanApplication().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> lendingCollateralValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewCollateral(value));

        var outbound2 = CompareUnderwritingOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CompareUnderwritingOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(reply1.getCollateral().getAddress().getLinesList())
                                .build())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getCollateral().getAudit().getEtag())
                                .build())
                        .addComments(UnderwritingComment.newBuilder()
                                .setCreatedAt(reply1.getCollateral().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var reply2 = RpcSupport.call(outbound2, value -> insuranceUnderwritingCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareUnderwritingOptions(value));
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "WatchRiskRegisters", request.getFilter().getQuery());
        return WatchRiskRegistersResponse.newBuilder()
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .setAudit(AuditInfo.newBuilder()
                                        .setEtag(reply0.getLoanApplication().getAudit().getEtag())
                                        .setUpdatedAt(reply0.getLoanApplication().getAudit().getUpdatedAt())
                                        .build())
                                .addChildren(RiskRegister.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .addAllLines(reply1.getCollateral().getAddress().getLinesList())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(reply2.getUnderwriting().getAssignee().getEmail())
                                                .build())
                                        .build())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setManager(UserRef.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .build())
                        .build())
                .build();
    }

    /**
     * Takes a stream of risk registers and answers once at the end.
     */
    public BatchCreateRiskRegistersResponse batchCreateRiskRegisters(BatchCreateRiskRegistersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BatchCreateRiskRegisters")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        if (request.hasRecord()) {
            repository.saveRiskRegister(request.getContext().getTenantId(), request.getRecord());
        }

        var outbound0 = CompareUnderwritingOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CompareUnderwritingOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(switch (request.getChunkCase()) {
                                    case RECORD -> request.getRecord().getLocation().getPostalCode();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(switch (request.getChunkCase()) {
                                    case RECORD -> request.getRecord().getAudit().getCreatedBy();
                                    default -> "";
                                })
                                .setUpdatedAt(request.getRecord().getAudit().getUpdatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(switch (request.getChunkCase()) {
                                    case RECORD -> request.getRecord().getManager().getEmail();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = insuranceUnderwritingCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareUnderwritingOptions(outbound0);

        var outbound1 = ResolveRiskRegistersRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var channel1 = channels.lookup("acme.risk.v1.RiskRegisterQueryService");
        var serviceName1 = "acme.risk.v1" + "." + "RiskRegisterQueryService";
        var methodName1 = "Res" + "olveRiskRegisters";
        var descriptor1 = MethodDescriptor.<ResolveRiskRegistersRequest, ResolveRiskRegistersResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(ResolveRiskRegistersRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ResolveRiskRegistersResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);
        log.atDebug().addKeyValue("auditValue", request.getHeader().getSource()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BatchCreateRiskRegisters", request.getHeader().getSource());
        return BatchCreateRiskRegistersResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Creates a risk register.
     */
    public CloneRiskRegisterResponse cloneRiskRegister(CloneRiskRegisterRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CloneRiskRegister")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskRegister(request.getContext().getTenantId(), request.getRiskRegister());

        var outbound0 = (ValidateRiskScenarioRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getRiskRegister().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = riskRiskScenarioValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateRiskScenario(outbound0);

        var outbound1 = mapper.cloneRiskRegisterStage1(request, reply0);
        var reply1 = venuesVenueValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintVenues(outbound1);

        var outbound2 = GetRiskRegisterRulesRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegisterId(request.getRiskRegister().getRiskRegisterId())
                .build();
        Function<GetRiskRegisterRulesRequest, GetRiskRegisterRulesResponse> operation2 = riskRiskRegisterValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::getRiskRegisterRules;
        var reply2 = operation2.apply(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getRiskRegister().getParentId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CloneRiskRegister", request.getRiskRegister().getParentId());
        return CloneRiskRegisterResponse.newBuilder()
                .setRiskRegister(RiskRegister.newBuilder()
                        .putAllAttributes(request.getRiskRegister().getAttributesMap())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getRiskScenario().getAudit().getCreatedAt())
                                .build())
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setUpdatedAt(reply1.getVenue().getAudit().getUpdatedAt())
                                                .build())
                                        .setRiskRegisterId(reply2.getRiskRegister().getRiskRegisterId())
                                        .build())
                                .build())
                        .setName(request.getRiskRegister().getName())
                        .setParentId(reply2.getRiskRegister().getParentId())
                        .build())
                .build();
    }
}
