package com.acme.finance.domains.lending;

import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CreateCustomerRequest;
import com.acme.customers.v1.CreateCustomerResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerPreferences;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerServiceGrpc;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.LookupCustomerRequest;
import com.acme.customers.v1.LookupCustomerResponse;
import com.acme.customers.v1.QuoteHouseholdRequest;
import com.acme.customers.v1.QuoteHouseholdResponse;
import com.acme.customers.v1.RecalculateHouseholdsRequest;
import com.acme.customers.v1.RecalculateHouseholdsResponse;
import com.acme.customers.v1.ScoreHouseholdRequest;
import com.acme.customers.v1.ScoreHouseholdResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.PeekUserRequest;
import com.acme.identity.v1.PeekUserResponse;
import com.acme.identity.v1.UserQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ApprovePremiumRequest;
import com.acme.insurance.v1.ApprovePremiumResponse;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumApprovalServiceGrpc;
import com.acme.insurance.v1.PremiumBankSettlement;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.ResolvePremiumConflictRequest;
import com.acme.insurance.v1.ResolvePremiumConflictResponse;
import com.acme.learning.v1.Course;
import com.acme.learning.v1.CourseServiceGrpc;
import com.acme.learning.v1.CreateCourseRequest;
import com.acme.learning.v1.CreateCourseResponse;
import com.acme.lending.v1.AdvanceLoanApplicationWorkflowRequest;
import com.acme.lending.v1.AdvanceLoanApplicationWorkflowResponse;
import com.acme.lending.v1.CancelLoanApplicationWorkflowRequest;
import com.acme.lending.v1.CancelLoanApplicationWorkflowResponse;
import com.acme.lending.v1.CheckLoanApplicationEligibilityRequest;
import com.acme.lending.v1.CheckLoanApplicationEligibilityResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralCalculationServiceGrpc;
import com.acme.lending.v1.CollateralComponent;
import com.acme.lending.v1.EstimateCollateralRequest;
import com.acme.lending.v1.EstimateCollateralResponse;
import com.acme.lending.v1.InspectLoanApplicationWorkflowRequest;
import com.acme.lending.v1.InspectLoanApplicationWorkflowResponse;
import com.acme.lending.v1.ListPendingLoanApplicationsRequest;
import com.acme.lending.v1.ListPendingLoanApplicationsResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationApprovalServiceGrpc;
import com.acme.lending.v1.LoanApplicationValidationServiceGrpc;
import com.acme.lending.v1.LoanApprovalServiceGrpc;
import com.acme.lending.v1.LoanLifecycleServiceGrpc;
import com.acme.lending.v1.LoanLimit;
import com.acme.lending.v1.PauseLoanApplicationWorkflowRequest;
import com.acme.lending.v1.PauseLoanApplicationWorkflowResponse;
import com.acme.lending.v1.ResumeLoanApplicationWorkflowRequest;
import com.acme.lending.v1.ResumeLoanApplicationWorkflowResponse;
import com.acme.lending.v1.RetryLoanApplicationWorkflowRequest;
import com.acme.lending.v1.RetryLoanApplicationWorkflowResponse;
import com.acme.lending.v1.ScheduleLoanExpiryRequest;
import com.acme.lending.v1.ScheduleLoanExpiryResponse;
import com.acme.lending.v1.SignalLoanApplicationWorkflowRequest;
import com.acme.lending.v1.SignalLoanApplicationWorkflowResponse;
import com.acme.lending.v1.StartLoanApplicationWorkflowRequest;
import com.acme.lending.v1.StartLoanApplicationWorkflowResponse;
import com.acme.lending.v1.SubmitLoanRequest;
import com.acme.lending.v1.SubmitLoanResponse;
import com.acme.risk.v1.BatchCreateRiskRegistersRequest;
import com.acme.risk.v1.BatchCreateRiskRegistersResponse;
import com.acme.risk.v1.RiskRegisterQueryServiceGrpc;
import com.acme.risk.v1.RiskRegisterServiceGrpc;
import com.acme.risk.v1.SummarizeRiskRegistersRequest;
import com.acme.risk.v1.SummarizeRiskRegistersResponse;
import com.google.protobuf.Any;
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
import org.springframework.stereotype.Service;

/**
 * Runs the long-lived workflow that takes a loan application from start to finish. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class LoanApplicationWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(LoanApplicationWorkflowApplication.class);

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking;

    private final CustomerServiceGrpc.CustomerServiceBlockingStub customersCustomerServiceBlocking;

    private final HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking;

    private final HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking;

    private final UserQueryServiceGrpc.UserQueryServiceBlockingStub identityUserQueryServiceBlocking;

    private final PremiumApprovalServiceGrpc.PremiumApprovalServiceBlockingStub insurancePremiumApprovalServiceBlocking;

    private final PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking;

    private final CourseServiceGrpc.CourseServiceBlockingStub learningCourseServiceBlocking;

    private final CollateralCalculationServiceGrpc.CollateralCalculationServiceBlockingStub lendingCollateralCalculationServiceBlocking;

    private final LoanApplicationApprovalServiceGrpc.LoanApplicationApprovalServiceBlockingStub lendingLoanApplicationApprovalServiceBlocking;

    private final LoanApplicationValidationServiceGrpc.LoanApplicationValidationServiceBlockingStub lendingLoanApplicationValidationServiceBlocking;

    private final LoanApprovalServiceGrpc.LoanApprovalServiceBlockingStub lendingLoanApprovalServiceBlocking;

    private final LoanLifecycleServiceGrpc.LoanLifecycleServiceBlockingStub lendingLoanLifecycleServiceBlocking;

    private final LoanApplicationWorkflowRequestMapper mapper;

    private final LendingRepository repository;

    private final RiskRegisterQueryServiceGrpc.RiskRegisterQueryServiceFutureStub riskRiskRegisterQueryServiceFuture;

    private final RiskRegisterServiceGrpc.RiskRegisterServiceStub riskRiskRegisterServiceAsync;

    private final Executor rpcExecutor;

    public LoanApplicationWorkflowApplication(
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking,
            CustomerServiceGrpc.CustomerServiceBlockingStub customersCustomerServiceBlocking,
            HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking,
            HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking,
            UserQueryServiceGrpc.UserQueryServiceBlockingStub identityUserQueryServiceBlocking,
            PremiumApprovalServiceGrpc.PremiumApprovalServiceBlockingStub insurancePremiumApprovalServiceBlocking,
            PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking,
            CourseServiceGrpc.CourseServiceBlockingStub learningCourseServiceBlocking,
            CollateralCalculationServiceGrpc.CollateralCalculationServiceBlockingStub lendingCollateralCalculationServiceBlocking,
            LoanApplicationApprovalServiceGrpc.LoanApplicationApprovalServiceBlockingStub lendingLoanApplicationApprovalServiceBlocking,
            LoanApplicationValidationServiceGrpc.LoanApplicationValidationServiceBlockingStub lendingLoanApplicationValidationServiceBlocking,
            LoanApprovalServiceGrpc.LoanApprovalServiceBlockingStub lendingLoanApprovalServiceBlocking,
            LoanLifecycleServiceGrpc.LoanLifecycleServiceBlockingStub lendingLoanLifecycleServiceBlocking,
            LoanApplicationWorkflowRequestMapper mapper,
            LendingRepository repository,
            RiskRegisterQueryServiceGrpc.RiskRegisterQueryServiceFutureStub riskRiskRegisterQueryServiceFuture,
            RiskRegisterServiceGrpc.RiskRegisterServiceStub riskRiskRegisterServiceAsync,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.customersCustomerQueryServiceBlocking = customersCustomerQueryServiceBlocking;
        this.customersCustomerServiceBlocking = customersCustomerServiceBlocking;
        this.customersHouseholdCalculationServiceBlocking = customersHouseholdCalculationServiceBlocking;
        this.customersHouseholdValidationServiceBlocking = customersHouseholdValidationServiceBlocking;
        this.identityUserQueryServiceBlocking = identityUserQueryServiceBlocking;
        this.insurancePremiumApprovalServiceBlocking = insurancePremiumApprovalServiceBlocking;
        this.insurancePremiumSyncServiceBlocking = insurancePremiumSyncServiceBlocking;
        this.learningCourseServiceBlocking = learningCourseServiceBlocking;
        this.lendingCollateralCalculationServiceBlocking = lendingCollateralCalculationServiceBlocking;
        this.lendingLoanApplicationApprovalServiceBlocking = lendingLoanApplicationApprovalServiceBlocking;
        this.lendingLoanApplicationValidationServiceBlocking = lendingLoanApplicationValidationServiceBlocking;
        this.lendingLoanApprovalServiceBlocking = lendingLoanApprovalServiceBlocking;
        this.lendingLoanLifecycleServiceBlocking = lendingLoanLifecycleServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.riskRiskRegisterQueryServiceFuture = riskRiskRegisterQueryServiceFuture;
        this.riskRiskRegisterServiceAsync = riskRiskRegisterServiceAsync;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Starts a loan application.
     */
    public StartLoanApplicationWorkflowResponse startLoanApplicationWorkflow(StartLoanApplicationWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartLoanApplicationWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoanApplication(request.getContext().getTenantId(), request.getLoanApplication());

        var outbound0 = EstimateCollateralRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setRegion(request.getLoanApplication().getBillingAddress().getRegion())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getLoanApplication().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getLoanApplication().getCounterparty().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        Function<EstimateCollateralRequest, EstimateCollateralResponse> operation0 = lendingCollateralCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::estimateCollateral;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = ApprovePremiumRequest.newBuilder()
                .setContext(request.getContext())
                .setPremium(Premium.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(switch (reply0.getCollateral().getLocatorCase()) {
                                    case ADDRESS -> reply0.getCollateral().getAddress().getLocality();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = insurancePremiumApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approvePremium(outbound1);

        var outbound2 = SummarizeRiskRegistersRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SummarizeRiskRegistersRequest.Details.newBuilder()
                        .putAllFilters(reply0.getResult().getDetailsMap())
                        .build())
                .setReason(request.getReason())
                .build();
        var pending2 = riskRiskRegisterQueryServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeRiskRegisters(outbound2);
        var reply2 = RpcSupport.await(pending2);

        var outbound3 = ListPendingLoanApplicationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingLoanApplicationsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getCollateral().getAudit().getUpdatedAt())
                        .setOwnerId(reply0.getCollateral().getComponentsOrDefault("primary", CollateralComponent.getDefaultInstance()).getComponentId())
                        .setRegion(switch (reply0.getCollateral().getLocatorCase()) {
                            case ADDRESS -> reply0.getCollateral().getAddress().getRegion();
                            default -> "";
                        })
                        .build())
                .build();
        var reply3 = lendingLoanApplicationApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingLoanApplications(outbound3);

        var outbound4 = BatchCreateRiskRegistersRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateRiskRegistersRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getNotify())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(BatchCreateRiskRegistersResponse.getDefaultInstance());
        var input4 = riskRiskRegisterServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateRiskRegisters(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();

        var outbound5 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getLoanApplication().getAudit().getCreatedAt())
                                .setCreatedBy(request.getLoanApplication().getAudit().getCreatedBy())
                                .setEtag(request.getLoanApplication().getAudit().getEtag())
                                .setUpdatedAt(request.getLoanApplication().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector5 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input5 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector5);
        input5.onNext(outbound5);
        input5.onCompleted();
        var reply5 = collector5.await();
        log.atDebug().addKeyValue("auditValue", request.getLoanApplication().getLoanApplicationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartLoanApplicationWorkflow", request.getLoanApplication().getLoanApplicationId());
        return StartLoanApplicationWorkflowResponse.newBuilder()
                .setLoanApplication(LoanApplication.newBuilder()
                        .setBillingAddress(reply1.getPremium().getBillingAddress())
                        .setDueAt(request.getLoanApplication().getDueAt())
                        .setLoanApplicationId(request.getLoanApplicationId())
                        .build())
                .setOutcome(StartLoanApplicationWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply3.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Advances a loan application.
     */
    public AdvanceLoanApplicationWorkflowResponse advanceLoanApplicationWorkflow(AdvanceLoanApplicationWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceLoanApplicationWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoanApplication(request.getContext().getTenantId(), request.getLoanApplication());

        var futures0 = request.getLoanApplication().getLinesList().stream()
                .map(element -> ScoreHouseholdRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .addChildren(Household.newBuilder()
                                                .putAllAttributes(element.getAttributesMap())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> customersHouseholdValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreHousehold(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? ScoreHouseholdResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = QuoteHouseholdRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setUpdatedBy(request.getLoanApplication().getAudit().getUpdatedBy())
                                                .build())
                                        .setLocation(PostalAddress.newBuilder()
                                                .addAllLines(reply0.getHousehold().getLocation().getLinesList())
                                                .setRecipient(request.getLoanApplication().getBillingAddress().getRecipient())
                                                .build())
                                        .build())
                                .setLocation(PostalAddress.newBuilder()
                                        .setPostalCode(request.getLoanApplication().getBillingAddress().getPostalCode())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(customersHouseholdCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteHousehold(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound2 = JsonCodec.roundTrip(CheckLoanApplicationEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getLoanApplication().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getLoanApplication().getBillingAddress().getCountryCode())
                                .setPostalCode(reply0.getHousehold().getLocation().getPostalCode())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(request.getLoanApplication().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build(), CheckLoanApplicationEligibilityRequest.class);
        var reply2 = lendingLoanApplicationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkLoanApplicationEligibility(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getLoanApplication().getLoanApplicationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceLoanApplicationWorkflow", request.getLoanApplication().getLoanApplicationId());
        return AdvanceLoanApplicationWorkflowResponse.newBuilder()
                .setLoanApplication(LoanApplication.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply1.getHousehold().getLocation().getLinesList())
                                .setPostalCode(reply1.getHousehold().getLocation().getPostalCode())
                                .setRecipient(reply2.getLoanApplication().getBillingAddress().getRecipient())
                                .build())
                        .setLoanApplicationId(request.getLoanApplicationId())
                        .setNumber(request.getLoanApplication().getNumber())
                        .build())
                .setOutcome(AdvanceLoanApplicationWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Cancels a loan application.
     */
    public CancelLoanApplicationWorkflowResponse cancelLoanApplicationWorkflow(CancelLoanApplicationWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelLoanApplicationWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoanApplication(request.getContext().getTenantId(), request.getLoanApplication());

        var outbound0 = CreateCourseRequest.newBuilder()
                .setContext(request.getContext())
                .setCourse(Course.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getLoanApplication().getBillingAddress().getRecipient())
                                .setRegion(request.getLoanApplication().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> learningCourseServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createCourse(value));

        var outbound1 = RecalculateHouseholdsRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .setCountryCode(request.getLoanApplication().getBillingAddress().getCountryCode())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(request.getLoanApplication().getCounterparty().getDisplayName())
                                                .setUserId(reply0.getCourse().getOwner().getUserId())
                                                .build())
                                        .build())
                                .setManager(UserRef.newBuilder()
                                        .setDisplayName(reply0.getCourse().getOwner().getDisplayName())
                                        .build())
                                .build())
                        .build())
                .build();
        RpcPort<RecalculateHouseholdsRequest, RecalculateHouseholdsResponse> port1 = RpcPort.adapt(customersHouseholdCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::recalculateHouseholds);
        var reply1 = port1.exchange(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getLoanApplication().getLoanApplicationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelLoanApplicationWorkflow", request.getLoanApplication().getLoanApplicationId());
        return CancelLoanApplicationWorkflowResponse.newBuilder()
                .setLoanApplication(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getCourse().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply0.getCourse().getAddress().getRegion())
                                .build())
                        .setIssuedAt(request.getLoanApplication().getIssuedAt())
                        .setLoanApplicationId(request.getLoanApplicationId())
                        .build())
                .setOutcome(CancelLoanApplicationWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Retrys a loan application.
     */
    public RetryLoanApplicationWorkflowResponse retryLoanApplicationWorkflow(RetryLoanApplicationWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryLoanApplicationWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoanApplication(request.getContext().getTenantId(), request.getLoanApplication());

        var outbound0 = RpcSupport.unpack(Any.pack(PeekUserRequest.newBuilder()
                .setContext(request.getContext())
                .setUserId(request.getContext().getActor().getUserId())
                .build()), PeekUserRequest.class);
        var reply0 = identityUserQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).peekUser(outbound0);

        var outbound1 = BeanCopies.copy(ScheduleLoanExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getLoanApplication().getAudit().getCreatedAt())
                                .setCreatedBy(reply0.getUser().getAudit().getCreatedBy())
                                .setEtag(request.getLoanApplication().getAudit().getEtag())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getUser().getRelationshipManager().getEmail())
                                .build())
                        .build())
                .build(), ScheduleLoanExpiryRequest.class);
        var reply1 = lendingLoanLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleLoanExpiry(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getLoanApplicationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryLoanApplicationWorkflow", request.getLoanApplicationId());
        return RetryLoanApplicationWorkflowResponse.newBuilder()
                .setLoanApplication(LoanApplication.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getUser().getPrimaryAddress().getPostalCode())
                                .build())
                        .setLoanApplicationId(reply1.getLoan().getHolder().getLoanApplicationId())
                        .setNumber(request.getLoanApplication().getNumber())
                        .setTotal(request.getLoanApplication().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Pauses a loan application.
     */
    public PauseLoanApplicationWorkflowResponse pauseLoanApplicationWorkflow(PauseLoanApplicationWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseLoanApplicationWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoanApplication(request.getContext().getTenantId(), request.getLoanApplication());

        var futures0 = request.getLoanApplication().getLinesList().stream()
                .map(element -> SubmitLoanRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubmitLoanRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getLoanApplication().getCounterparty().getEmail())
                                .build())
                        .build())
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getLoanApplication().getAudit().getEtag())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .build())
                        .addLimits(LoanLimit.newBuilder()
                                .setAmount(element.getAmount())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> lendingLoanApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitLoan(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? SubmitLoanResponse.getDefaultInstance()
                : results0.getLast();
        log.atDebug().addKeyValue("auditValue", request.getLoanApplication().getLoanApplicationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseLoanApplicationWorkflow", request.getLoanApplication().getLoanApplicationId());
        return PauseLoanApplicationWorkflowResponse.newBuilder()
                .setLoanApplication(LoanApplication.newBuilder()
                        .setDueAt(request.getLoanApplication().getDueAt())
                        .setLoanApplicationId(request.getLoanApplicationId())
                        .build())
                .build();
    }

    /**
     * Resumes a loan application.
     */
    public ResumeLoanApplicationWorkflowResponse resumeLoanApplicationWorkflow(ResumeLoanApplicationWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeLoanApplicationWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoanApplication(request.getContext().getTenantId(), request.getLoanApplication());

        var outbound0 = mapper.resumeLoanApplicationWorkflowStage0(request);
        var reply0 = customersCustomerQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lookupCustomer(outbound0);

        var outbound1 = CreateCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setPreferences(CustomerPreferences.newBuilder()
                                .addAllChannels(reply0.getCustomer().getPreferences().getChannelsList())
                                .build())
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setRegion(reply0.getCustomer().getPrimaryAddress().getRegion())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> customersCustomerServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createCustomer(value));

        var outbound2 = ResolvePremiumConflictRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ResolvePremiumConflictRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setPremium(Premium.newBuilder()
                        .setBank(PremiumBankSettlement.newBuilder()
                                .setIban(switch (request.getLoanApplication().getSettlementCase()) {
                                    case BANK -> request.getLoanApplication().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getCustomer().getPrimaryAddress().getCountryCode())
                                .addAllLines(reply1.getCustomer().getPrimaryAddress().getLinesList())
                                .setRegion(reply0.getCustomer().getPrimaryAddress().getRegion())
                                .build())
                        .build())
                .build();
        var reply2 = insurancePremiumSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolvePremiumConflict(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getLoanApplication().getLoanApplicationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeLoanApplicationWorkflow", request.getLoanApplication().getLoanApplicationId());
        return ResumeLoanApplicationWorkflowResponse.newBuilder()
                .setLoanApplication(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getCustomer().getAudit().getCreatedBy())
                                .setEtag(reply0.getCustomer().getAudit().getEtag())
                                .build())
                        .setLoanApplicationId(request.getLoanApplicationId())
                        .setTotal(request.getLoanApplication().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Returns a loan application.
     */
    public InspectLoanApplicationWorkflowResponse inspectLoanApplicationWorkflow(InspectLoanApplicationWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectLoanApplicationWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LoanApplication", request.getLoanApplicationId(), "InspectLoanApplicationWorkflow");
        return InspectLoanApplicationWorkflowResponse.newBuilder()
                .setLoanApplication(LoanApplication.newBuilder()
                        .setLoanApplicationId(request.getLoanApplicationId())
                        .build())
                .build();
    }

    /**
     * Signals a loan application.
     */
    public SignalLoanApplicationWorkflowResponse signalLoanApplicationWorkflow(SignalLoanApplicationWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalLoanApplicationWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoanApplication(request.getContext().getTenantId(), request.getLoanApplication());

        var outbound0 = CreateCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .build())
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getLoanApplication().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(customersCustomerServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createCustomer(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getLoanApplication().getLoanApplicationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalLoanApplicationWorkflow", request.getLoanApplication().getLoanApplicationId());
        return SignalLoanApplicationWorkflowResponse.newBuilder()
                .setLoanApplication(LoanApplication.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getCustomer().getPrimaryAddress().getLinesList())
                                .build())
                        .setDueAt(request.getLoanApplication().getDueAt())
                        .setLoanApplicationId(request.getLoanApplicationId())
                        .build())
                .build();
    }
}
