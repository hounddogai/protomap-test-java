package com.acme.finance.domains.insurance;

import com.acme.assets.v1.FixedAssetQueryServiceGrpc;
import com.acme.assets.v1.ResolveFixedAssetsRequest;
import com.acme.assets.v1.ResolveFixedAssetsResponse;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailSyncServiceGrpc;
import com.acme.audit.v1.PullAuditTrailsRequest;
import com.acme.audit.v1.PullAuditTrailsResponse;
import com.acme.audit.v1.RetentionPolicyRef;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.compliance.v1.BrowseComplianceCasesRequest;
import com.acme.compliance.v1.BrowseComplianceCasesResponse;
import com.acme.compliance.v1.ComplianceCaseQueryServiceGrpc;
import com.acme.content.v1.ArticleServiceGrpc;
import com.acme.content.v1.ListArticlesRequest;
import com.acme.content.v1.ListArticlesResponse;
import com.acme.customers.v1.CalculateHouseholdRequest;
import com.acme.customers.v1.CalculateHouseholdResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.HouseholdQueryServiceGrpc;
import com.acme.customers.v1.SuggestHouseholdsRequest;
import com.acme.customers.v1.SuggestHouseholdsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ApproveClaimRequest;
import com.acme.insurance.v1.ApproveClaimResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.CommentClaimApprovalRequest;
import com.acme.insurance.v1.CommentClaimApprovalResponse;
import com.acme.insurance.v1.CountPremiumTotalsRequest;
import com.acme.insurance.v1.CountPremiumTotalsResponse;
import com.acme.insurance.v1.CountUnderwritingTotalsRequest;
import com.acme.insurance.v1.CountUnderwritingTotalsResponse;
import com.acme.insurance.v1.DelegateClaimApprovalRequest;
import com.acme.insurance.v1.DelegateClaimApprovalResponse;
import com.acme.insurance.v1.DeletePolicyRequest;
import com.acme.insurance.v1.DeletePolicyResponse;
import com.acme.insurance.v1.EscalateClaimRequest;
import com.acme.insurance.v1.EscalateClaimResponse;
import com.acme.insurance.v1.ListCoverageReportsRequest;
import com.acme.insurance.v1.ListCoverageReportsResponse;
import com.acme.insurance.v1.ListPendingClaimsRequest;
import com.acme.insurance.v1.ListPendingClaimsResponse;
import com.acme.insurance.v1.ListPremiumReportsRequest;
import com.acme.insurance.v1.ListPremiumReportsResponse;
import com.acme.insurance.v1.PolicyServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumReportingServiceGrpc;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.PullPremiumsRequest;
import com.acme.insurance.v1.PullPremiumsResponse;
import com.acme.insurance.v1.RecallClaimRequest;
import com.acme.insurance.v1.RecallClaimResponse;
import com.acme.insurance.v1.RejectClaimRequest;
import com.acme.insurance.v1.RejectClaimResponse;
import com.acme.insurance.v1.StreamPremiumMetricsRequest;
import com.acme.insurance.v1.StreamPremiumMetricsResponse;
import com.acme.insurance.v1.SubmitClaimRequest;
import com.acme.insurance.v1.SubmitClaimResponse;
import com.acme.insurance.v1.UnderwritingReportingServiceGrpc;
import com.acme.loyalty.v1.ExplainLoyaltyMemberCalculationRequest;
import com.acme.loyalty.v1.ExplainLoyaltyMemberCalculationResponse;
import com.acme.loyalty.v1.LoyaltyMember;
import com.acme.loyalty.v1.LoyaltyMemberCalculationServiceGrpc;
import com.acme.loyalty.v1.RewardRef;
import com.acme.procurement.v1.NormalizeRfqRequest;
import com.acme.procurement.v1.NormalizeRfqResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqSla;
import com.acme.procurement.v1.RfqValidationServiceGrpc;
import com.acme.suppliers.v1.BatchGetSuppliersRequest;
import com.acme.suppliers.v1.BatchGetSuppliersResponse;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.acme.treasury.v1.BankTransferQueryServiceGrpc;
import com.acme.treasury.v1.CountBankTransfersRequest;
import com.acme.treasury.v1.CountBankTransfersResponse;
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
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Routes claims through approval, with escalation and delegation. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class ClaimApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(ClaimApprovalApplication.class);

    private final FixedAssetQueryServiceGrpc.FixedAssetQueryServiceFutureStub assetsFixedAssetQueryServiceFuture;

    private final AuditTrailSyncServiceGrpc.AuditTrailSyncServiceBlockingStub auditAuditTrailSyncServiceBlocking;

    private final ChannelRegistry channels;

    private final ComplianceCaseQueryServiceGrpc.ComplianceCaseQueryServiceBlockingStub complianceComplianceCaseQueryServiceBlocking;

    private final ArticleServiceGrpc.ArticleServiceBlockingStub contentArticleServiceBlocking;

    private final HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking;

    private final HouseholdQueryServiceGrpc.HouseholdQueryServiceBlockingStub customersHouseholdQueryServiceBlocking;

    private final ApplicationEventPublisher events;

    private final PolicyServiceGrpc.PolicyServiceBlockingStub insurancePolicyServiceBlocking;

    private final PremiumReportingServiceGrpc.PremiumReportingServiceBlockingStub insurancePremiumReportingServiceBlocking;

    private final PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking;

    private final UnderwritingReportingServiceGrpc.UnderwritingReportingServiceBlockingStub insuranceUnderwritingReportingServiceBlocking;

    private final LoyaltyMemberCalculationServiceGrpc.LoyaltyMemberCalculationServiceBlockingStub loyaltyLoyaltyMemberCalculationServiceBlocking;

    private final ClaimApprovalRequestMapper mapper;

    private final RfqValidationServiceGrpc.RfqValidationServiceBlockingStub procurementRfqValidationServiceBlocking;

    private final InsuranceRepository repository;

    private final Executor rpcExecutor;

    private final SupplierServiceGrpc.SupplierServiceFutureStub suppliersSupplierServiceFuture;

    public ClaimApprovalApplication(
            FixedAssetQueryServiceGrpc.FixedAssetQueryServiceFutureStub assetsFixedAssetQueryServiceFuture,
            AuditTrailSyncServiceGrpc.AuditTrailSyncServiceBlockingStub auditAuditTrailSyncServiceBlocking,
            ChannelRegistry channels,
            ComplianceCaseQueryServiceGrpc.ComplianceCaseQueryServiceBlockingStub complianceComplianceCaseQueryServiceBlocking,
            ArticleServiceGrpc.ArticleServiceBlockingStub contentArticleServiceBlocking,
            HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking,
            HouseholdQueryServiceGrpc.HouseholdQueryServiceBlockingStub customersHouseholdQueryServiceBlocking,
            ApplicationEventPublisher events,
            PolicyServiceGrpc.PolicyServiceBlockingStub insurancePolicyServiceBlocking,
            PremiumReportingServiceGrpc.PremiumReportingServiceBlockingStub insurancePremiumReportingServiceBlocking,
            PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking,
            UnderwritingReportingServiceGrpc.UnderwritingReportingServiceBlockingStub insuranceUnderwritingReportingServiceBlocking,
            LoyaltyMemberCalculationServiceGrpc.LoyaltyMemberCalculationServiceBlockingStub loyaltyLoyaltyMemberCalculationServiceBlocking,
            ClaimApprovalRequestMapper mapper,
            RfqValidationServiceGrpc.RfqValidationServiceBlockingStub procurementRfqValidationServiceBlocking,
            InsuranceRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierServiceGrpc.SupplierServiceFutureStub suppliersSupplierServiceFuture) {
        this.assetsFixedAssetQueryServiceFuture = assetsFixedAssetQueryServiceFuture;
        this.auditAuditTrailSyncServiceBlocking = auditAuditTrailSyncServiceBlocking;
        this.channels = channels;
        this.complianceComplianceCaseQueryServiceBlocking = complianceComplianceCaseQueryServiceBlocking;
        this.contentArticleServiceBlocking = contentArticleServiceBlocking;
        this.customersHouseholdCalculationServiceBlocking = customersHouseholdCalculationServiceBlocking;
        this.customersHouseholdQueryServiceBlocking = customersHouseholdQueryServiceBlocking;
        this.events = events;
        this.insurancePolicyServiceBlocking = insurancePolicyServiceBlocking;
        this.insurancePremiumReportingServiceBlocking = insurancePremiumReportingServiceBlocking;
        this.insurancePremiumSyncServiceBlocking = insurancePremiumSyncServiceBlocking;
        this.insuranceUnderwritingReportingServiceBlocking = insuranceUnderwritingReportingServiceBlocking;
        this.loyaltyLoyaltyMemberCalculationServiceBlocking = loyaltyLoyaltyMemberCalculationServiceBlocking;
        this.mapper = mapper;
        this.procurementRfqValidationServiceBlocking = procurementRfqValidationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierServiceFuture = suppliersSupplierServiceFuture;
    }

    public record ListPendingClaimsStage0Requested(ListCoverageReportsRequest request, CompletableFuture<ListCoverageReportsResponse> completion, io.grpc.Context context) {}

    /**
     * Submits a claim.
     */
    public SubmitClaimResponse submitClaim(SubmitClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveClaim(request.getContext().getTenantId(), request.getClaim());

        var results0 = new ArrayList<StreamPremiumMetricsResponse>();
        for (var element : request.getClaim().getCommentsList()) {
            StreamPremiumMetricsRequest itemRequest = StreamPremiumMetricsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamPremiumMetricsRequest.Filter.newBuilder()
                        .setCreatedAfter(element.getCreatedAt())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(element.getAuthorEmail())
                        .setName(request.getClaim().getRequester().getName())
                        .setPhone(request.getClaim().getRequester().getPhone())
                        .build())
                .build();
            var responses = insurancePremiumReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamPremiumMetrics(itemRequest);
            responses.forEachRemaining(results0::add);
        }
        var reply0 = results0.isEmpty() ? StreamPremiumMetricsResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = CountPremiumTotalsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountPremiumTotalsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getClaim().getAudit().getCreatedAt())
                        .setOwnerId(request.getClaim().getSla().getPolicyId())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.insurance.v1.PremiumReportingService/CountPremiumTotals", outbound1);
        var reply1 = insurancePremiumReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countPremiumTotals(command1.payload());

        var outbound2 = BrowseComplianceCasesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseComplianceCasesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getClaim().getSla().getDueAt())
                        .setOwnerId(switch (request.getClaim().getOriginCase()) {
                            case CUSTOMER -> request.getClaim().getCustomer().getCustomerId();
                            default -> "";
                        })
                        .build())
                .build();
        var reply2 = complianceComplianceCaseQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseComplianceCases(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getClaim().getClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitClaim", request.getClaim().getClaimId());
        return SubmitClaimResponse.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setClaimId(request.getClaimId())
                        .putAllCustomFields(request.getClaim().getCustomFieldsMap())
                        .build())
                .setOutcome(SubmitClaimResponse.Outcome.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Approves a claim.
     */
    public ApproveClaimResponse approveClaim(ApproveClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveClaim(request.getContext().getTenantId(), request.getClaim());

        List<SuggestHouseholdsResponse> results0 = request.getClaim().getCommentsList().stream()
                .map(element -> SuggestHouseholdsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestHouseholdsRequest.Filter.newBuilder()
                        .setCreatedAfter(element.getCreatedAt())
                        .build())
                .build())
                .flatMap(value -> Stream.of(customersHouseholdQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestHouseholds(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? SuggestHouseholdsResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = CalculateHouseholdRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedBy(request.getClaim().getAudit().getCreatedBy())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(switch (request.getClaim().getOriginCase()) {
                                                    case CUSTOMER -> request.getClaim().getCustomer().getDisplayName();
                                                    default -> "";
                                                })
                                                .build())
                                        .build())
                                .setManager(UserRef.newBuilder()
                                        .setEmail(request.getDetails().getApprover().getEmail())
                                        .build())
                                .build())
                        .setManager(UserRef.newBuilder()
                                .setEmail(request.getClaim().getAssignee().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = customersHouseholdCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateHousehold(outbound1);

        var outbound2 = ResolveFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending2 = assetsFixedAssetQueryServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).resolveFixedAssets(outbound2);
        var reply2 = RpcSupport.await(pending2);
        log.atDebug().addKeyValue("auditValue", request.getClaim().getClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveClaim", request.getClaim().getClaimId());
        return ApproveClaimResponse.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply1.getHousehold().getManager().getDisplayName())
                                .build())
                        .setClaimId(request.getClaimId())
                        .setStatus(request.getClaim().getStatus())
                        .build())
                .setOutcome(ApproveClaimResponse.Outcome.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Rejects a claim.
     */
    public RejectClaimResponse rejectClaim(RejectClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveClaim(request.getContext().getTenantId(), request.getClaim());

        var outbound0 = ExplainLoyaltyMemberCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setLoyaltyMember(LoyaltyMember.newBuilder()
                        .setHolder(RewardRef.newBuilder()
                                .setDisplayName(request.getClaim().getAssignee().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(loyaltyLoyaltyMemberCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainLoyaltyMemberCalculation(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getClaim().getClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectClaim", request.getClaim().getClaimId());
        return RejectClaimResponse.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(request.getClaim().getAssignee())
                        .setClaimId(request.getClaimId())
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(reply0.getLoyaltyMember().getContact().getPhone())
                                .build())
                        .build())
                .setOutcome(RejectClaimResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Escalates a claim.
     */
    public EscalateClaimResponse escalateClaim(EscalateClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveClaim(request.getContext().getTenantId(), request.getClaim());

        var outbound0 = CountUnderwritingTotalsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountUnderwritingTotalsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getClaim().getAudit().getUpdatedAt())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> insuranceUnderwritingReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countUnderwritingTotals(outbound0), rpcExecutor);

        var outbound1 = DeletePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setEtag(request.getClaim().getAudit().getEtag())
                .setPolicyId(request.getClaim().getSla().getPolicyId())
                .setReason(request.getReason())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> insurancePolicyServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).deletePolicy(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getClaim().getClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateClaim", request.getClaim().getClaimId());
        return EscalateClaimResponse.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setClaimId(request.getClaimId())
                        .setRequester(request.getClaim().getRequester())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Lists claims a page at a time.
     */
    public ListPendingClaimsResponse listPendingClaims(ListPendingClaimsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingClaims")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Claim", request.getContext().getTraceId(), "ListPendingClaims");

        var outbound0 = ListCoverageReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListCoverageReportsRequest.Filter.newBuilder()
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build();
        var completion0 = new CompletableFuture<ListCoverageReportsResponse>();
        events.publishEvent(new ListPendingClaimsStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingClaims", request.getPage().getPageToken());
        return ListPendingClaimsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Delegates a claim.
     */
    public DelegateClaimApprovalResponse delegateClaimApproval(DelegateClaimApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateClaimApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveClaim(request.getContext().getTenantId(), request.getClaim());

        var outbound0 = BeanCopies.copy(ListPremiumReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPremiumReportsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getClaim().getAudit().getUpdatedAt())
                        .build())
                .build(), ListPremiumReportsRequest.class);
        var reply0 = insurancePremiumReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPremiumReports(outbound0);

        var outbound1 = mapper.delegateClaimApprovalStage1(request);
        var reply1 = auditAuditTrailSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pullAuditTrails(outbound1);

        var outbound2 = BatchGetSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending2 = suppliersSupplierServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetSuppliers(outbound2);
        var reply2 = RpcSupport.await(pending2);

        var outbound3 = CountUnderwritingTotalsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountUnderwritingTotalsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply1.getAuditTrail().getAudit().getUpdatedAt())
                        .setRegion(reply1.getAuditTrail().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply3 = RpcSupport.call(outbound3, value -> insuranceUnderwritingReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countUnderwritingTotals(value));
        log.atDebug().addKeyValue("auditValue", request.getClaim().getClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateClaimApproval", request.getClaim().getClaimId());
        return DelegateClaimApprovalResponse.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setClaimId(request.getClaimId())
                        .setSubject(request.getClaim().getSubject())
                        .build())
                .setOutcome(DelegateClaimApprovalResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply3.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Recalls a claim.
     */
    public RecallClaimResponse recallClaim(RecallClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveClaim(request.getContext().getTenantId(), request.getClaim());

        var outbound0 = ListArticlesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListArticlesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getClaim().getAudit().getUpdatedAt())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> contentArticleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listArticles(value));

        var outbound1 = CountBankTransfersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountBankTransfersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getClaim().getAudit().getCreatedAt())
                        .setOwnerId(request.getClaim().getAssignee().getUserId())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.treasury.v1.BankTransferQueryService");
        var selected1 = BankTransferQueryServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.countBankTransfers(outbound1);

        var outbound2 = NormalizeRfqRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeRfqRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .setRfq(Rfq.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getClaim().getAudit().getUpdatedBy())
                                .build())
                        .setSla(RfqSla.newBuilder()
                                .setDueAt(request.getClaim().getSla().getDueAt())
                                .build())
                        .build())
                .build();
        var reply2 = procurementRfqValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeRfq(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getClaim().getClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallClaim", request.getClaim().getClaimId());
        return RecallClaimResponse.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setClaimId(request.getClaimId())
                        .setPriority(reply2.getRfq().getPriority())
                        .build())
                .setOutcome(RecallClaimResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Comments a claim.
     */
    public CommentClaimApprovalResponse commentClaimApproval(CommentClaimApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentClaimApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveClaim(request.getContext().getTenantId(), request.getClaim());

        var outbound0 = PullPremiumsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PullPremiumsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getClaim().getAudit().getCreatedAt())
                                .setUpdatedAt(request.getClaim().getAudit().getUpdatedAt())
                                .setUpdatedBy(request.getClaim().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setDisplayName(switch (request.getClaim().getOriginCase()) {
                                    case CUSTOMER -> request.getClaim().getCustomer().getDisplayName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = insurancePremiumSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pullPremiums(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getClaim().getClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentClaimApproval", request.getClaim().getClaimId());
        return CommentClaimApprovalResponse.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply0.getPremium().getCounterparty().getEmail())
                                .build())
                        .setClaimId(request.getClaimId())
                        .putAllCustomFields(request.getClaim().getCustomFieldsMap())
                        .build())
                .build();
    }
}
