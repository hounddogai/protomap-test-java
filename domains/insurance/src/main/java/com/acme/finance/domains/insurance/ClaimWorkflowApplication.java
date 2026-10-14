package com.acme.finance.domains.insurance;

import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.billing.v1.PaymentTerm;
import com.acme.billing.v1.PaymentTermCalculationServiceGrpc;
import com.acme.billing.v1.QuotePaymentTermRequest;
import com.acme.billing.v1.QuotePaymentTermResponse;
import com.acme.budgeting.v1.BatchCreateBudgetsRequest;
import com.acme.budgeting.v1.BatchCreateBudgetsResponse;
import com.acme.budgeting.v1.BudgetServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseComment;
import com.acme.compliance.v1.ComplianceCaseServiceGrpc;
import com.acme.compliance.v1.CreateComplianceCaseRequest;
import com.acme.compliance.v1.CreateComplianceCaseResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordMeasurement;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.ExplainCustomerSegmentCalculationRequest;
import com.acme.customers.v1.ExplainCustomerSegmentCalculationResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.customers.v1.PreviewHouseholdRequest;
import com.acme.customers.v1.PreviewHouseholdResponse;
import com.acme.customers.v1.PreviewPreferenceRequest;
import com.acme.customers.v1.PreviewPreferenceResponse;
import com.acme.customers.v1.ScoreConsentRecordRequest;
import com.acme.customers.v1.ScoreConsentRecordResponse;
import com.acme.drivers.v1.DriverRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.identity.v1.WatchUsersRequest;
import com.acme.identity.v1.WatchUsersResponse;
import com.acme.insurance.v1.AdvanceClaimWorkflowRequest;
import com.acme.insurance.v1.AdvanceClaimWorkflowResponse;
import com.acme.insurance.v1.ApprovePolicyRequest;
import com.acme.insurance.v1.ApprovePolicyResponse;
import com.acme.insurance.v1.CancelClaimWorkflowRequest;
import com.acme.insurance.v1.CancelClaimWorkflowResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimSla;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageValidationServiceGrpc;
import com.acme.insurance.v1.GetPolicyApprovalStatusRequest;
import com.acme.insurance.v1.GetPolicyApprovalStatusResponse;
import com.acme.insurance.v1.GetPolicyRequest;
import com.acme.insurance.v1.GetPolicyResponse;
import com.acme.insurance.v1.InspectClaimWorkflowRequest;
import com.acme.insurance.v1.InspectClaimWorkflowResponse;
import com.acme.insurance.v1.ListClaimWorkflowsRequest;
import com.acme.insurance.v1.ListClaimWorkflowsResponse;
import com.acme.insurance.v1.NormalizeCoverageRequest;
import com.acme.insurance.v1.NormalizeCoverageResponse;
import com.acme.insurance.v1.PauseClaimWorkflowRequest;
import com.acme.insurance.v1.PauseClaimWorkflowResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyApprovalServiceGrpc;
import com.acme.insurance.v1.PolicyServiceGrpc;
import com.acme.insurance.v1.PolicyWorkflowServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.PreviewCoverageRequest;
import com.acme.insurance.v1.PreviewCoverageResponse;
import com.acme.insurance.v1.RejectPolicyRequest;
import com.acme.insurance.v1.RejectPolicyResponse;
import com.acme.insurance.v1.ResolvePremiumConflictRequest;
import com.acme.insurance.v1.ResolvePremiumConflictResponse;
import com.acme.insurance.v1.ResumeClaimWorkflowRequest;
import com.acme.insurance.v1.ResumeClaimWorkflowResponse;
import com.acme.insurance.v1.RetryClaimWorkflowRequest;
import com.acme.insurance.v1.RetryClaimWorkflowResponse;
import com.acme.insurance.v1.SignalClaimWorkflowRequest;
import com.acme.insurance.v1.SignalClaimWorkflowResponse;
import com.acme.insurance.v1.StartClaimWorkflowRequest;
import com.acme.insurance.v1.StartClaimWorkflowResponse;
import com.acme.insurance.v1.StartPolicyWorkflowRequest;
import com.acme.insurance.v1.StartPolicyWorkflowResponse;
import com.acme.insurance.v1.SyncPremiumsRequest;
import com.acme.insurance.v1.SyncPremiumsResponse;
import com.acme.payroll.v1.BrowsePayRunsRequest;
import com.acme.payroll.v1.BrowsePayRunsResponse;
import com.acme.payroll.v1.PayRunQueryServiceGrpc;
import com.acme.payroll.v1.SummarizePayRunsRequest;
import com.acme.payroll.v1.SummarizePayRunsResponse;
import com.acme.routing.v1.Route;
import com.acme.routing.v1.RouteServiceGrpc;
import com.acme.routing.v1.UpdateRouteRequest;
import com.acme.routing.v1.UpdateRouteResponse;
import com.acme.suppliers.v1.ExplainSupplierRiskDecisionRequest;
import com.acme.suppliers.v1.ExplainSupplierRiskDecisionResponse;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.SupplierRiskSla;
import com.acme.suppliers.v1.SupplierRiskValidationServiceGrpc;
import com.acme.venues.v1.SearchVenuesRequest;
import com.acme.venues.v1.SearchVenuesResponse;
import com.acme.venues.v1.VenueServiceGrpc;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Runs the long-lived workflow that takes a claim from start to finish. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class ClaimWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(ClaimWorkflowApplication.class);

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final PaymentTermCalculationServiceGrpc.PaymentTermCalculationServiceBlockingStub billingPaymentTermCalculationServiceBlocking;

    private final BudgetServiceGrpc.BudgetServiceStub budgetingBudgetServiceAsync;

    private final ChannelRegistry channels;

    private final ComplianceCaseServiceGrpc.ComplianceCaseServiceBlockingStub complianceComplianceCaseServiceBlocking;

    private final ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceBlockingStub customersConsentRecordValidationServiceBlocking;

    private final CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking;

    private final HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking;

    private final PreferenceValidationServiceGrpc.PreferenceValidationServiceBlockingStub customersPreferenceValidationServiceBlocking;

    private final UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking;

    private final CoverageValidationServiceGrpc.CoverageValidationServiceBlockingStub insuranceCoverageValidationServiceBlocking;

    private final PolicyApprovalServiceGrpc.PolicyApprovalServiceBlockingStub insurancePolicyApprovalServiceBlocking;

    private final PolicyServiceGrpc.PolicyServiceBlockingStub insurancePolicyServiceBlocking;

    private final PolicyWorkflowServiceGrpc.PolicyWorkflowServiceBlockingStub insurancePolicyWorkflowServiceBlocking;

    private final PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking;

    private final PayRunQueryServiceGrpc.PayRunQueryServiceBlockingStub payrollPayRunQueryServiceBlocking;

    private final PayRunQueryServiceGrpc.PayRunQueryServiceFutureStub payrollPayRunQueryServiceFuture;

    private final InsuranceRepository repository;

    private final RouteServiceGrpc.RouteServiceBlockingStub routingRouteServiceBlocking;

    private final Executor rpcExecutor;

    private final SupplierRiskValidationServiceGrpc.SupplierRiskValidationServiceBlockingStub suppliersSupplierRiskValidationServiceBlocking;

    private final VenueServiceGrpc.VenueServiceBlockingStub venuesVenueServiceBlocking;

    public ClaimWorkflowApplication(
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            PaymentTermCalculationServiceGrpc.PaymentTermCalculationServiceBlockingStub billingPaymentTermCalculationServiceBlocking,
            BudgetServiceGrpc.BudgetServiceStub budgetingBudgetServiceAsync,
            ChannelRegistry channels,
            ComplianceCaseServiceGrpc.ComplianceCaseServiceBlockingStub complianceComplianceCaseServiceBlocking,
            ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceBlockingStub customersConsentRecordValidationServiceBlocking,
            CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking,
            HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking,
            PreferenceValidationServiceGrpc.PreferenceValidationServiceBlockingStub customersPreferenceValidationServiceBlocking,
            UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking,
            CoverageValidationServiceGrpc.CoverageValidationServiceBlockingStub insuranceCoverageValidationServiceBlocking,
            PolicyApprovalServiceGrpc.PolicyApprovalServiceBlockingStub insurancePolicyApprovalServiceBlocking,
            PolicyServiceGrpc.PolicyServiceBlockingStub insurancePolicyServiceBlocking,
            PolicyWorkflowServiceGrpc.PolicyWorkflowServiceBlockingStub insurancePolicyWorkflowServiceBlocking,
            PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking,
            PayRunQueryServiceGrpc.PayRunQueryServiceBlockingStub payrollPayRunQueryServiceBlocking,
            PayRunQueryServiceGrpc.PayRunQueryServiceFutureStub payrollPayRunQueryServiceFuture,
            InsuranceRepository repository,
            RouteServiceGrpc.RouteServiceBlockingStub routingRouteServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierRiskValidationServiceGrpc.SupplierRiskValidationServiceBlockingStub suppliersSupplierRiskValidationServiceBlocking,
            VenueServiceGrpc.VenueServiceBlockingStub venuesVenueServiceBlocking) {
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.billingPaymentTermCalculationServiceBlocking = billingPaymentTermCalculationServiceBlocking;
        this.budgetingBudgetServiceAsync = budgetingBudgetServiceAsync;
        this.channels = channels;
        this.complianceComplianceCaseServiceBlocking = complianceComplianceCaseServiceBlocking;
        this.customersConsentRecordValidationServiceBlocking = customersConsentRecordValidationServiceBlocking;
        this.customersCustomerSegmentCalculationServiceBlocking = customersCustomerSegmentCalculationServiceBlocking;
        this.customersHouseholdValidationServiceBlocking = customersHouseholdValidationServiceBlocking;
        this.customersPreferenceValidationServiceBlocking = customersPreferenceValidationServiceBlocking;
        this.identityUserServiceBlocking = identityUserServiceBlocking;
        this.insuranceCoverageValidationServiceBlocking = insuranceCoverageValidationServiceBlocking;
        this.insurancePolicyApprovalServiceBlocking = insurancePolicyApprovalServiceBlocking;
        this.insurancePolicyServiceBlocking = insurancePolicyServiceBlocking;
        this.insurancePolicyWorkflowServiceBlocking = insurancePolicyWorkflowServiceBlocking;
        this.insurancePremiumSyncServiceBlocking = insurancePremiumSyncServiceBlocking;
        this.payrollPayRunQueryServiceBlocking = payrollPayRunQueryServiceBlocking;
        this.payrollPayRunQueryServiceFuture = payrollPayRunQueryServiceFuture;
        this.repository = repository;
        this.routingRouteServiceBlocking = routingRouteServiceBlocking;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierRiskValidationServiceBlocking = suppliersSupplierRiskValidationServiceBlocking;
        this.venuesVenueServiceBlocking = venuesVenueServiceBlocking;
    }

    /**
     * Starts a claim.
     */
    public StartClaimWorkflowResponse startClaimWorkflow(StartClaimWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartClaimWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveClaim(request.getContext().getTenantId(), request.getClaim());

        var outbound0 = SummarizePayRunsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> payrollPayRunQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizePayRuns(outbound0), rpcExecutor);

        var outbound1 = QuotePaymentTermRequest.newBuilder()
                .setContext(request.getContext())
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getClaim().getAssignee().getUserId())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> billingPaymentTermCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePaymentTerm(outbound1), rpcExecutor);

        var outbound2 = NormalizeCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeCoverageRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getClaim().getRequester().getName())
                                .build())
                        .build())
                .build();
        var pending2 = CompletableFuture.supplyAsync(() -> insuranceCoverageValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeCoverage(outbound2), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1, pending2).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        var reply2 = pending2.join();

        var outbound3 = BatchCreateBudgetsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateBudgetsRequest.StreamHeader.newBuilder()
                        .setSource(request.getClaim().getAssignee().getUserId())
                        .setSubmittedBy(request.getClaim().getRequester())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(BatchCreateBudgetsResponse.getDefaultInstance());
        var input3 = budgetingBudgetServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateBudgets(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();

        var outbound4 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getClaim().getAudit().getCreatedAt())
                                .setCreatedBy(request.getClaim().getAudit().getCreatedBy())
                                .setEtag(request.getClaim().getAudit().getEtag())
                                .setUpdatedAt(request.getClaim().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input4 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();
        log.atDebug().addKeyValue("auditValue", request.getClaim().getClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartClaimWorkflow", request.getClaim().getClaimId());
        return StartClaimWorkflowResponse.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply1.getPaymentTerm().getOwner().getDisplayName())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getPaymentTerm().getAudit().getEtag())
                                .setUpdatedAt(reply2.getCoverage().getAudit().getUpdatedAt())
                                .build())
                        .setClaimId(request.getClaimId())
                        .setPriority(request.getClaim().getPriority())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Advances a claim.
     */
    public AdvanceClaimWorkflowResponse advanceClaimWorkflow(AdvanceClaimWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceClaimWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveClaim(request.getContext().getTenantId(), request.getClaim());

        var results0 = new ArrayList<SyncPremiumsResponse>();
        for (var element : request.getClaim().getHistoryList()) {
            SyncPremiumsRequest itemRequest = SyncPremiumsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SyncPremiumsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getChangedAt())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(request.getClaim().getCustomer().getEmail())
                                .build())
                        .build())
                .build();
            results0.add(insurancePremiumSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).syncPremiums(itemRequest));
        }
        var reply0 = results0.isEmpty() ? SyncPremiumsResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = PreviewHouseholdRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedBy(request.getClaim().getAudit().getCreatedBy())
                                                .setUpdatedBy(reply0.getPremium().getAudit().getUpdatedBy())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(request.getClaim().getAssignee().getEmail())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply1 = customersHouseholdValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewHousehold(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getClaim().getClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceClaimWorkflow", request.getClaim().getClaimId());
        return AdvanceClaimWorkflowResponse.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setClaimId(request.getClaimId())
                        .setSubject(request.getClaim().getSubject())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Cancels a claim.
     */
    public CancelClaimWorkflowResponse cancelClaimWorkflow(CancelClaimWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelClaimWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveClaim(request.getContext().getTenantId(), request.getClaim());

        var outbound0 = (UpdateRouteRequest.newBuilder()
                .setContext(request.getContext())
                .setRoute(Route.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getClaim().getAudit().getUpdatedAt())
                                .build())
                        .setCounterparty(DriverRef.newBuilder()
                                .setEmail(request.getClaim().getCustomer().getEmail())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = routingRouteServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateRoute(outbound0);

        var results1 = new ArrayList<CreateComplianceCaseResponse>();
        for (var element : request.getClaim().getCommentsList()) {
            CreateComplianceCaseRequest itemRequest = CreateComplianceCaseRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .addComments(ComplianceCaseComment.newBuilder()
                                .setAuthorEmail(element.getAuthorEmail())
                                .setVisibility(element.getVisibility())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
            results1.add(complianceComplianceCaseServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createComplianceCase(itemRequest));
        }
        var reply1 = results1.isEmpty() ? CreateComplianceCaseResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getClaim().getClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelClaimWorkflow", request.getClaim().getClaimId());
        return CancelClaimWorkflowResponse.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getRoute().getAudit().getUpdatedBy())
                                .build())
                        .setClaimId(request.getClaimId())
                        .setPriority(request.getClaim().getPriority())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Retrys a claim.
     */
    public RetryClaimWorkflowResponse retryClaimWorkflow(RetryClaimWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryClaimWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveClaim(request.getContext().getTenantId(), request.getClaim());

        var outbound0 = WatchUsersRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setName(request.getClaim().getRequester().getName())
                        .build())
                .build();
        var stream0 = identityUserServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchUsers(outbound0);
        var reply0 = WatchUsersResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = RejectPolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RejectPolicyRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setUserId(request.getClaim().getAssignee().getUserId())
                                .build())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getClaim().getAudit().getCreatedAt())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(switch (request.getClaim().getOriginCase()) {
                                    case CUSTOMER -> request.getClaim().getCustomer().getEmail();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = insurancePolicyApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectPolicy(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getClaim().getClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryClaimWorkflow", request.getClaim().getClaimId());
        return RetryClaimWorkflowResponse.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setClaimId(request.getClaimId())
                        .setPriority(request.getClaim().getPriority())
                        .setSla(ClaimSla.newBuilder()
                                .setPolicyId(reply1.getPolicy().getPolicyId())
                                .build())
                        .build())
                .build();
    }

    /**
     * Pauses a claim.
     */
    public PauseClaimWorkflowResponse pauseClaimWorkflow(PauseClaimWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseClaimWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveClaim(request.getContext().getTenantId(), request.getClaim());

        var outbound0 = ScoreConsentRecordRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .addMeasurements(ConsentRecordMeasurement.newBuilder()
                                .setObservedAt(request.getClaim().getAudit().getCreatedAt())
                                .build())
                        .setSubject(HouseholdRef.newBuilder()
                                .setDisplayName(request.getClaim().getAssignee().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> customersConsentRecordValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreConsentRecord(outbound0), rpcExecutor);

        var outbound1 = ExplainSupplierRiskDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainSupplierRiskDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getClaim().getRequester().getEmail())
                                .build())
                        .build())
                .setSupplierRisk(SupplierRisk.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getClaim().getAudit().getCreatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .build())
                        .setSla(SupplierRiskSla.newBuilder()
                                .setDueAt(request.getClaim().getSla().getDueAt())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> suppliersSupplierRiskValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainSupplierRiskDecision(outbound1), rpcExecutor);

        var outbound2 = StartPolicyWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(StartPolicyWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getClaim().getAudit().getCreatedBy())
                                .setEtag(request.getClaim().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var pending2 = CompletableFuture.supplyAsync(() -> insurancePolicyWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).startPolicyWorkflow(outbound2), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1, pending2).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        var reply2 = pending2.join();

        List<PreviewPreferenceResponse> results3 = request.getClaim().getCommentsList().stream()
                .map(element -> PreviewPreferenceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewPreferenceRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotify().getName())
                                .build())
                        .build())
                .setPreference(Preference.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getClaim().getAssignee().getDisplayName())
                                .setEmail(element.getAuthorEmail())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(customersPreferenceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewPreference(value)))
                .collect(Collectors.toList());
        var reply3 = results3.isEmpty() ? PreviewPreferenceResponse.getDefaultInstance()
                : results3.getLast();
        log.atDebug().addKeyValue("auditValue", request.getClaim().getClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseClaimWorkflow", request.getClaim().getClaimId());
        return PauseClaimWorkflowResponse.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply2.getPolicy().getAudit().getUpdatedBy())
                                .build())
                        .setClaimId(request.getClaimId())
                        .putAllCustomFields(request.getClaim().getCustomFieldsMap())
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(reply1.getSupplierRisk().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .setOutcome(PauseClaimWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Resumes a claim.
     */
    public ResumeClaimWorkflowResponse resumeClaimWorkflow(ResumeClaimWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeClaimWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveClaim(request.getContext().getTenantId(), request.getClaim());

        var futures0 = request.getClaim().getHistoryList().stream()
                .map(element -> SearchVenuesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchVenuesRequest.Filter.newBuilder()
                        .setCreatedAfter(element.getChangedAt())
                        .setOwnerId(request.getClaim().getAssignee().getUserId())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> venuesVenueServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchVenues(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? SearchVenuesResponse.getDefaultInstance()
                : results0.getLast();

        var futures1 = request.getClaim().getCommentsList().stream()
                .map(element -> ResolvePremiumConflictRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ResolvePremiumConflictRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> insurancePremiumSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolvePremiumConflict(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? ResolvePremiumConflictResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = BrowsePayRunsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowsePayRunsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply1.getPremium().getAudit().getUpdatedAt())
                        .setOwnerId(request.getClaim().getSla().getPolicyId())
                        .setRegion(reply1.getPremium().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending2 = payrollPayRunQueryServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).browsePayRuns(outbound2);
        var reply2 = RpcSupport.await(pending2);
        log.atDebug().addKeyValue("auditValue", request.getClaim().getClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeClaimWorkflow", request.getClaim().getClaimId());
        return ResumeClaimWorkflowResponse.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setClaimId(request.getClaimId())
                        .setPriority(request.getClaim().getPriority())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a claim.
     */
    public InspectClaimWorkflowResponse inspectClaimWorkflow(InspectClaimWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectClaimWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Claim", request.getClaimId(), "InspectClaimWorkflow");

        var outbound0 = ApprovePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ApprovePolicyRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setExternalAccountId(request.getClaimId())
                        .setOpenedAt(request.getContext().getDeadline())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.insurance.v1.PolicyApprovalService");
        var selected0 = PolicyApprovalServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.approvePolicy(outbound0);
        return InspectClaimWorkflowResponse.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getPolicy().getAudit().getUpdatedBy())
                                .build())
                        .setClaimId(request.getClaimId())
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(reply0.getPolicy().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
    }

    /**
     * Lists claims a page at a time.
     */
    public ListClaimWorkflowsResponse listClaimWorkflows(ListClaimWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListClaimWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Claim", request.getContext().getTraceId(), "ListClaimWorkflows");

        var outbound0 = ExplainCustomerSegmentCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setDetails(ExplainCustomerSegmentCalculationRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build();
        final class Stage0Adapter {
            ExplainCustomerSegmentCalculationResponse fetch(ExplainCustomerSegmentCalculationRequest input) {
                return customersCustomerSegmentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainCustomerSegmentCalculation(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListClaimWorkflows", request.getPage().getPageToken());
        return ListClaimWorkflowsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Signals a claim.
     */
    public SignalClaimWorkflowResponse signalClaimWorkflow(SignalClaimWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalClaimWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveClaim(request.getContext().getTenantId(), request.getClaim());

        var outbound0 = GetPolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicyId(request.getClaim().getSla().getPolicyId())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> insurancePolicyServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getPolicy(outbound0), rpcExecutor);

        var outbound1 = GetPolicyApprovalStatusRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicyId(request.getClaim().getSla().getPolicyId())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> insurancePolicyApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getPolicyApprovalStatus(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = PreviewCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getPolicy().getAudit().getEtag())
                                .setUpdatedBy(request.getClaim().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getPolicy().getContact().getEmail())
                                .build())
                        .build())
                .build();
        RpcPort<PreviewCoverageRequest, PreviewCoverageResponse> port2 = RpcPort.adapt(insuranceCoverageValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::previewCoverage);
        var reply2 = port2.exchange(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getClaim().getClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalClaimWorkflow", request.getClaim().getClaimId());
        return SignalClaimWorkflowResponse.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply2.getCoverage().getOwner().getEmail())
                                .build())
                        .setClaimId(request.getClaimId())
                        .setRequester(ContactInfo.newBuilder()
                                .setName(reply0.getPolicy().getContact().getName())
                                .setPhone(reply0.getPolicy().getContact().getPhone())
                                .build())
                        .setSubject(request.getClaim().getSubject())
                        .build())
                .setOutcome(SignalClaimWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }
}
