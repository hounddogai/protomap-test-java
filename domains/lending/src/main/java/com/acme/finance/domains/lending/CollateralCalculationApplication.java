package com.acme.finance.domains.lending;

import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.NotifyAuditEventWatchersRequest;
import com.acme.audit.v1.NotifyAuditEventWatchersResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.CountAccessPoliciesRequest;
import com.acme.authorization.v1.CountAccessPoliciesResponse;
import com.acme.banking.v1.ExportStatementsRequest;
import com.acme.banking.v1.ExportStatementsResponse;
import com.acme.banking.v1.StatementQueryServiceGrpc;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseServiceGrpc;
import com.acme.collections.v1.UpdateCollectionCaseRequest;
import com.acme.collections.v1.UpdateCollectionCaseResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimCalculationServiceGrpc;
import com.acme.insurance.v1.ExplainClaimCalculationRequest;
import com.acme.insurance.v1.ExplainClaimCalculationResponse;
import com.acme.lending.v1.CalculateCollateralRequest;
import com.acme.lending.v1.CalculateCollateralResponse;
import com.acme.lending.v1.CalculateLoanRequest;
import com.acme.lending.v1.CalculateLoanResponse;
import com.acme.lending.v1.CheckLoanApplicationEligibilityRequest;
import com.acme.lending.v1.CheckLoanApplicationEligibilityResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralComponent;
import com.acme.lending.v1.CompareCollateralOptionsRequest;
import com.acme.lending.v1.CompareCollateralOptionsResponse;
import com.acme.lending.v1.EstimateCollateralRequest;
import com.acme.lending.v1.EstimateCollateralResponse;
import com.acme.lending.v1.ExplainLoanApplicationDecisionRequest;
import com.acme.lending.v1.ExplainLoanApplicationDecisionResponse;
import com.acme.lending.v1.GetCollateralRatesRequest;
import com.acme.lending.v1.GetCollateralRatesResponse;
import com.acme.lending.v1.LintLoanApplicationsRequest;
import com.acme.lending.v1.LintLoanApplicationsResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationCalculationServiceGrpc;
import com.acme.lending.v1.LoanApplicationLine;
import com.acme.lending.v1.LoanApplicationValidationServiceGrpc;
import com.acme.lending.v1.LoanCalculationServiceGrpc;
import com.acme.lending.v1.QuoteCollateralRequest;
import com.acme.lending.v1.QuoteCollateralResponse;
import com.acme.lending.v1.QuoteLoanApplicationRequest;
import com.acme.lending.v1.QuoteLoanApplicationResponse;
import com.acme.lending.v1.SimulateCollateralRequest;
import com.acme.lending.v1.SimulateCollateralResponse;
import com.google.protobuf.Any;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Computes the figures of a collateral, such as totals, taxes, and estimates. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class CollateralCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(CollateralCalculationApplication.class);

    private final AuditEventNotificationServiceGrpc.AuditEventNotificationServiceBlockingStub auditAuditEventNotificationServiceBlocking;

    private final AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking;

    private final StatementQueryServiceGrpc.StatementQueryServiceBlockingStub bankingStatementQueryServiceBlocking;

    private final CollectionCaseServiceGrpc.CollectionCaseServiceBlockingStub collectionsCollectionCaseServiceBlocking;

    private final ClaimCalculationServiceGrpc.ClaimCalculationServiceBlockingStub insuranceClaimCalculationServiceBlocking;

    private final LoanApplicationCalculationServiceGrpc.LoanApplicationCalculationServiceBlockingStub lendingLoanApplicationCalculationServiceBlocking;

    private final LoanApplicationValidationServiceGrpc.LoanApplicationValidationServiceBlockingStub lendingLoanApplicationValidationServiceBlocking;

    private final LoanCalculationServiceGrpc.LoanCalculationServiceBlockingStub lendingLoanCalculationServiceBlocking;

    private final CollateralCalculationRequestMapper mapper;

    private final LendingRepository repository;

    private final Executor rpcExecutor;

    public CollateralCalculationApplication(
            AuditEventNotificationServiceGrpc.AuditEventNotificationServiceBlockingStub auditAuditEventNotificationServiceBlocking,
            AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking,
            StatementQueryServiceGrpc.StatementQueryServiceBlockingStub bankingStatementQueryServiceBlocking,
            CollectionCaseServiceGrpc.CollectionCaseServiceBlockingStub collectionsCollectionCaseServiceBlocking,
            ClaimCalculationServiceGrpc.ClaimCalculationServiceBlockingStub insuranceClaimCalculationServiceBlocking,
            LoanApplicationCalculationServiceGrpc.LoanApplicationCalculationServiceBlockingStub lendingLoanApplicationCalculationServiceBlocking,
            LoanApplicationValidationServiceGrpc.LoanApplicationValidationServiceBlockingStub lendingLoanApplicationValidationServiceBlocking,
            LoanCalculationServiceGrpc.LoanCalculationServiceBlockingStub lendingLoanCalculationServiceBlocking,
            CollateralCalculationRequestMapper mapper,
            LendingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.auditAuditEventNotificationServiceBlocking = auditAuditEventNotificationServiceBlocking;
        this.auditAuditEventServiceAsync = auditAuditEventServiceAsync;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.authorizationAccessPolicyQueryServiceBlocking = authorizationAccessPolicyQueryServiceBlocking;
        this.bankingStatementQueryServiceBlocking = bankingStatementQueryServiceBlocking;
        this.collectionsCollectionCaseServiceBlocking = collectionsCollectionCaseServiceBlocking;
        this.insuranceClaimCalculationServiceBlocking = insuranceClaimCalculationServiceBlocking;
        this.lendingLoanApplicationCalculationServiceBlocking = lendingLoanApplicationCalculationServiceBlocking;
        this.lendingLoanApplicationValidationServiceBlocking = lendingLoanApplicationValidationServiceBlocking;
        this.lendingLoanCalculationServiceBlocking = lendingLoanCalculationServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Calculates a collateral.
     */
    public CalculateCollateralResponse calculateCollateral(CalculateCollateralRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateCollateral")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollateral(request.getContext().getTenantId(), request.getCollateral());

        var outbound0 = mapper.calculateCollateralStage0(request);
        var reply0 = lendingLoanApplicationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkLoanApplicationEligibility(outbound0);

        var outbound1 = JsonCodec.roundTrip(CalculateLoanRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CalculateLoanRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getCollateral().getAddress().getLocality())
                                .build())
                        .build())
                .build(), CalculateLoanRequest.class);
        var reply1 = lendingLoanCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateLoan(outbound1);

        var outbound2 = BatchCreateAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateAuditEventsRequest.StreamHeader.newBuilder()
                        .setSource(request.getCollateral().getCollateralId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getCollateral().getOwner().getEmail())
                                .build())
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
                                .setCreatedAt(request.getCollateral().getAudit().getCreatedAt())
                                .setCreatedBy(request.getCollateral().getAudit().getCreatedBy())
                                .setEtag(request.getCollateral().getAudit().getEtag())
                                .setUpdatedAt(request.getCollateral().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input3 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getCollateral().getCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateCollateral", request.getCollateral().getCode());
        return CalculateCollateralResponse.newBuilder()
                .setBreakdown(CalculateCollateralResponse.Breakdown.newBuilder()
                        .setTotal(reply1.getBreakdown().getTotal())
                        .build())
                .setCollateral(Collateral.newBuilder()
                        .setCollateralId(request.getCollateralId())
                        .setName(request.getCollateral().getName())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Estimates a collateral.
     */
    public EstimateCollateralResponse estimateCollateral(EstimateCollateralRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateCollateral")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollateral(request.getContext().getTenantId(), request.getCollateral());

        var outbound0 = RpcSupport.unpack(Any.pack(UpdateCollectionCaseRequest.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getCollateral().getOwner().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getCollateral().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build()), UpdateCollectionCaseRequest.class);
        var reply0 = collectionsCollectionCaseServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateCollectionCase(outbound0);

        var outbound1 = (ExplainLoanApplicationDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getDetails().getDestination().getCountryCode())
                                .build())
                        .addLines(LoanApplicationLine.newBuilder()
                                .setDescription(request.getCollateral().getComponentsOrDefault("primary", CollateralComponent.getDefaultInstance()).getDescription())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply1 = lendingLoanApplicationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainLoanApplicationDecision(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getCollateral().getCollateralId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateCollateral", request.getCollateral().getCollateralId());
        return EstimateCollateralResponse.newBuilder()
                .setBreakdown(EstimateCollateralResponse.Breakdown.newBuilder()
                        .setTotal(reply1.getLoanApplication().getTotal())
                        .build())
                .setCollateral(Collateral.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getCollectionCase().getAudit().getEtag())
                                .build())
                        .setCode(request.getCollateral().getCode())
                        .setCollateralId(request.getCollateralId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Quotes a collateral.
     */
    public QuoteCollateralResponse quoteCollateral(QuoteCollateralRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuoteCollateral")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollateral(request.getContext().getTenantId(), request.getCollateral());

        var outbound0 = LintLoanApplicationsRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getDetails().getDestination().getPostalCode())
                                .build())
                        .addLines(LoanApplicationLine.newBuilder()
                                .setDescription(request.getCollateral().getComponentsOrDefault("primary", CollateralComponent.getDefaultInstance()).getDescription())
                                .build())
                        .build())
                .build();
        RpcPort<LintLoanApplicationsRequest, LintLoanApplicationsResponse> port0 = RpcPort.adapt(lendingLoanApplicationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::lintLoanApplications);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = ExportStatementsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportStatementsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getCollateral().getAudit().getCreatedAt())
                        .setOwnerId(request.getCollateral().getOwner().getUserId())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var stream1 = bankingStatementQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportStatements(outbound1);
        var reply1 = ExportStatementsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getCollateral().getCollateralId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuoteCollateral", request.getCollateral().getCollateralId());
        return QuoteCollateralResponse.newBuilder()
                .setBreakdown(QuoteCollateralResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .build())
                .setCollateral(Collateral.newBuilder()
                        .setCollateralId(request.getCollateralId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Compares a collateral.
     */
    public CompareCollateralOptionsResponse compareCollateralOptions(CompareCollateralOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareCollateralOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollateral(request.getContext().getTenantId(), request.getCollateral());

        var outbound0 = NotifyAuditEventWatchersRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NotifyAuditEventWatchersRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getCollateral().getOwner().getEmail())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build();
        RpcPort<NotifyAuditEventWatchersRequest, NotifyAuditEventWatchersResponse> port0 = RpcPort.adapt(auditAuditEventNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::notifyAuditEventWatchers);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = CheckLoanApplicationEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getCollateral().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(switch (request.getCollateral().getLocatorCase()) {
                                    case ADDRESS -> request.getCollateral().getAddress().getLocality();
                                    default -> "";
                                })
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .build())
                        .build())
                .build();
        var reply1 = lendingLoanApplicationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkLoanApplicationEligibility(outbound1);

        var outbound2 = ExplainClaimCalculationRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getCollateral().getOwner().getUserId())
                                .build())
                        .setCustomer(CustomerRef.newBuilder()
                                .setDisplayName(reply0.getAuditEvent().getSubject().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply2 = insuranceClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainClaimCalculation(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getCollateral().getCollateralId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareCollateralOptions", request.getCollateral().getCollateralId());
        return CompareCollateralOptionsResponse.newBuilder()
                .setBreakdown(CompareCollateralOptionsResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .setTotal(reply1.getLoanApplication().getTotal())
                        .build())
                .setCollateral(Collateral.newBuilder()
                        .setCollateralId(request.getCollateralId())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply2.getClaim().getCustomer().getDisplayName())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Simulates a collateral.
     */
    public SimulateCollateralResponse simulateCollateral(SimulateCollateralRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateCollateral")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollateral(request.getContext().getTenantId(), request.getCollateral());

        var outbound0 = QuoteLoanApplicationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(QuoteLoanApplicationRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getCollateral().getAddress().getCountryCode())
                                .build())
                        .build())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getCollateral().getAudit().getUpdatedAt())
                                .setUpdatedBy(request.getCollateral().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getDetails().getDestination().getPostalCode())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> lendingLoanApplicationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteLoanApplication(outbound0), rpcExecutor);

        var outbound1 = CountAccessPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountAccessPoliciesRequest.Filter.newBuilder()
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> authorizationAccessPolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countAccessPolicies(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getCollateral().getCollateralId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateCollateral", request.getCollateral().getCollateralId());
        return SimulateCollateralResponse.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setCapacity(request.getCollateral().getCapacity())
                        .setCollateralId(request.getCollateralId())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getLoanApplication().getCounterparty().getEmail())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a collateral.
     */
    public GetCollateralRatesResponse getCollateralRates(GetCollateralRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetCollateralRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Collateral", request.getCollateralId(), "GetCollateralRates");
        return GetCollateralRatesResponse.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setCollateralId(request.getCollateralId())
                        .build())
                .build();
    }
}
