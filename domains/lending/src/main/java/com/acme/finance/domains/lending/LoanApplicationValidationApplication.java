package com.acme.finance.domains.lending;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailSyncServiceGrpc;
import com.acme.audit.v1.PullAuditTrailsRequest;
import com.acme.audit.v1.PullAuditTrailsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.ScoreRetentionPolicyRequest;
import com.acme.audit.v1.ScoreRetentionPolicyResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.documents.v1.DocumentServiceGrpc;
import com.acme.documents.v1.ListDocumentsRequest;
import com.acme.documents.v1.ListDocumentsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.FindUsersRequest;
import com.acme.identity.v1.FindUsersResponse;
import com.acme.identity.v1.UserQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumCalculationServiceGrpc;
import com.acme.insurance.v1.PremiumCardSettlement;
import com.acme.insurance.v1.RecalculatePremiumsRequest;
import com.acme.insurance.v1.RecalculatePremiumsResponse;
import com.acme.lending.v1.CalculateLoanRequest;
import com.acme.lending.v1.CalculateLoanResponse;
import com.acme.lending.v1.CheckLoanApplicationEligibilityRequest;
import com.acme.lending.v1.CheckLoanApplicationEligibilityResponse;
import com.acme.lending.v1.CompareLoanOptionsRequest;
import com.acme.lending.v1.CompareLoanOptionsResponse;
import com.acme.lending.v1.EstimateLoanApplicationRequest;
import com.acme.lending.v1.EstimateLoanApplicationResponse;
import com.acme.lending.v1.ExplainLoanApplicationCalculationRequest;
import com.acme.lending.v1.ExplainLoanApplicationCalculationResponse;
import com.acme.lending.v1.ExplainLoanApplicationDecisionRequest;
import com.acme.lending.v1.ExplainLoanApplicationDecisionResponse;
import com.acme.lending.v1.ExplainLoanCalculationRequest;
import com.acme.lending.v1.ExplainLoanCalculationResponse;
import com.acme.lending.v1.GetLoanApplicationRulesRequest;
import com.acme.lending.v1.GetLoanApplicationRulesResponse;
import com.acme.lending.v1.LintLoanApplicationsRequest;
import com.acme.lending.v1.LintLoanApplicationsResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationCalculationServiceGrpc;
import com.acme.lending.v1.LoanApplicationRef;
import com.acme.lending.v1.LoanBankFunding;
import com.acme.lending.v1.LoanCalculationServiceGrpc;
import com.acme.lending.v1.LoanCardFunding;
import com.acme.lending.v1.NormalizeLoanApplicationRequest;
import com.acme.lending.v1.NormalizeLoanApplicationResponse;
import com.acme.lending.v1.PreviewLoanApplicationRequest;
import com.acme.lending.v1.PreviewLoanApplicationResponse;
import com.acme.lending.v1.RecalculateLoansRequest;
import com.acme.lending.v1.RecalculateLoansResponse;
import com.acme.lending.v1.SimulateLoanRequest;
import com.acme.lending.v1.SimulateLoanResponse;
import com.acme.lending.v1.ValidateLoanApplicationRequest;
import com.acme.lending.v1.ValidateLoanApplicationResponse;
import com.acme.search.v1.CountSearchIndexesRequest;
import com.acme.search.v1.CountSearchIndexesResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.google.protobuf.Any;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Checks loan applications against the rules before anything acts on them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class LoanApplicationValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(LoanApplicationValidationApplication.class);

    private final AuditTrailSyncServiceGrpc.AuditTrailSyncServiceBlockingStub auditAuditTrailSyncServiceBlocking;

    private final DocumentServiceGrpc.DocumentServiceBlockingStub documentsDocumentServiceBlocking;

    private final ApplicationEventPublisher events;

    private final UserQueryServiceGrpc.UserQueryServiceBlockingStub identityUserQueryServiceBlocking;

    private final PremiumCalculationServiceGrpc.PremiumCalculationServiceBlockingStub insurancePremiumCalculationServiceBlocking;

    private final LoanApplicationCalculationServiceGrpc.LoanApplicationCalculationServiceBlockingStub lendingLoanApplicationCalculationServiceBlocking;

    private final LoanCalculationServiceGrpc.LoanCalculationServiceBlockingStub lendingLoanCalculationServiceBlocking;

    private final LendingRepository repository;

    private final Executor rpcExecutor;

    private final SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking;

    public LoanApplicationValidationApplication(
            AuditTrailSyncServiceGrpc.AuditTrailSyncServiceBlockingStub auditAuditTrailSyncServiceBlocking,
            DocumentServiceGrpc.DocumentServiceBlockingStub documentsDocumentServiceBlocking,
            ApplicationEventPublisher events,
            UserQueryServiceGrpc.UserQueryServiceBlockingStub identityUserQueryServiceBlocking,
            PremiumCalculationServiceGrpc.PremiumCalculationServiceBlockingStub insurancePremiumCalculationServiceBlocking,
            LoanApplicationCalculationServiceGrpc.LoanApplicationCalculationServiceBlockingStub lendingLoanApplicationCalculationServiceBlocking,
            LoanCalculationServiceGrpc.LoanCalculationServiceBlockingStub lendingLoanCalculationServiceBlocking,
            LendingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking) {
        this.auditAuditTrailSyncServiceBlocking = auditAuditTrailSyncServiceBlocking;
        this.documentsDocumentServiceBlocking = documentsDocumentServiceBlocking;
        this.events = events;
        this.identityUserQueryServiceBlocking = identityUserQueryServiceBlocking;
        this.insurancePremiumCalculationServiceBlocking = insurancePremiumCalculationServiceBlocking;
        this.lendingLoanApplicationCalculationServiceBlocking = lendingLoanApplicationCalculationServiceBlocking;
        this.lendingLoanCalculationServiceBlocking = lendingLoanCalculationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.searchSearchIndexQueryServiceBlocking = searchSearchIndexQueryServiceBlocking;
    }

    public record PreviewLoanApplicationStage1Requested(ScoreRetentionPolicyRequest request, CompletableFuture<ScoreRetentionPolicyResponse> completion, io.grpc.Context context) {}

    public record PreviewLoanApplicationStage2Requested(SimulateLoanRequest request, CompletableFuture<SimulateLoanResponse> completion, io.grpc.Context context) {}

    /**
     * Validates a loan application.
     */
    public ValidateLoanApplicationResponse validateLoanApplication(ValidateLoanApplicationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateLoanApplication")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoanApplication(request.getContext().getTenantId(), request.getLoanApplication());

        var outbound0 = CalculateLoanRequest.newBuilder()
                .setContext(request.getContext())
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getLoanApplication().getAudit().getCreatedBy())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> lendingLoanCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateLoan(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getLoanApplication().getLoanApplicationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateLoanApplication", request.getLoanApplication().getLoanApplicationId());
        return ValidateLoanApplicationResponse.newBuilder()
                .setLoanApplication(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getLoan().getAudit().getCreatedAt())
                                .setUpdatedAt(reply0.getLoan().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(request.getLoanApplication().getBillingAddress())
                        .setLoanApplicationId(request.getLoanApplicationId())
                        .build())
                .build();
    }

    /**
     * Previews a loan application.
     */
    public PreviewLoanApplicationResponse previewLoanApplication(PreviewLoanApplicationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewLoanApplication")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoanApplication(request.getContext().getTenantId(), request.getLoanApplication());

        var outbound0 = RecalculateLoansRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecalculateLoansRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getLoanApplication().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getLoanApplication().getAudit().getCreatedBy())
                                .build())
                        .setCard(LoanCardFunding.newBuilder()
                                .setToken(switch (request.getLoanApplication().getSettlementCase()) {
                                    case CARD -> request.getLoanApplication().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = lendingLoanCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateLoans(outbound0);

        var outbound1 = ScoreRetentionPolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScoreRetentionPolicyRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getLoanApplication().getBillingAddress().getRegion())
                                .build())
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getLoanApplication().getAudit().getCreatedAt())
                                .setEtag(reply0.getLoan().getAudit().getEtag())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getLoan().getContact().getEmail())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<ScoreRetentionPolicyResponse>();
        events.publishEvent(new PreviewLoanApplicationStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound2 = SimulateLoanRequest.newBuilder()
                .setContext(request.getContext())
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getRetentionPolicy().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var completion2 = new CompletableFuture<SimulateLoanResponse>();
        events.publishEvent(new PreviewLoanApplicationStage2Requested(outbound2, completion2, io.grpc.Context.current()));
        var reply2 = completion2.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getLoanApplication().getLoanApplicationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewLoanApplication", request.getLoanApplication().getLoanApplicationId());
        return PreviewLoanApplicationResponse.newBuilder()
                .setLoanApplication(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getRetentionPolicy().getAudit().getEtag())
                                .build())
                        .setBillingAddress(request.getLoanApplication().getBillingAddress())
                        .setLoanApplicationId(request.getLoanApplicationId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Checks a loan application.
     */
    public CheckLoanApplicationEligibilityResponse checkLoanApplicationEligibility(CheckLoanApplicationEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckLoanApplicationEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoanApplication(request.getContext().getTenantId(), request.getLoanApplication());

        var outbound0 = RpcSupport.unpack(Any.pack(ExplainLoanApplicationCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainLoanApplicationCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getContact().getName())
                                .build())
                        .build())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(request.getLoanApplication().getBillingAddress().getLocality())
                                .build())
                        .build())
                .build()), ExplainLoanApplicationCalculationRequest.class);
        var reply0 = lendingLoanApplicationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainLoanApplicationCalculation(outbound0);

        var builder1 = CountSearchIndexesRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setFilter(CountSearchIndexesRequest.Filter.newBuilder()
                .setCreatedAfter(reply0.getLoanApplication().getAudit().getUpdatedAt())
                .setOwnerId(reply0.getResult().getCorrelationId())
                .setRegion(reply0.getLoanApplication().getBillingAddress().getRegion())
                .build());
        var outbound1 = builder1.build();
        var reply1 = searchSearchIndexQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countSearchIndexes(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getLoanApplication().getLoanApplicationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckLoanApplicationEligibility", request.getLoanApplication().getLoanApplicationId());
        return CheckLoanApplicationEligibilityResponse.newBuilder()
                .setLoanApplication(LoanApplication.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply0.getLoanApplication().getBillingAddress().getRecipient())
                                .build())
                        .setLoanApplicationId(request.getLoanApplicationId())
                        .setNumber(request.getLoanApplication().getNumber())
                        .build())
                .setOutcome(CheckLoanApplicationEligibilityResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Normalizes a loan application.
     */
    public NormalizeLoanApplicationResponse normalizeLoanApplication(NormalizeLoanApplicationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeLoanApplication")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoanApplication(request.getContext().getTenantId(), request.getLoanApplication());

        var outbound0 = CompareLoanOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CompareLoanOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getLoanApplication().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .setLoan(Loan.newBuilder()
                        .setBank(LoanBankFunding.newBuilder()
                                .setAccountHolder(switch (request.getLoanApplication().getSettlementCase()) {
                                    case BANK -> request.getLoanApplication().getBank().getAccountHolder();
                                    default -> "";
                                })
                                .build())
                        .setHolder(LoanApplicationRef.newBuilder()
                                .setDisplayName(request.getLoanApplication().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply0 = lendingLoanCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareLoanOptions(outbound0);

        var outbound1 = RecalculatePremiumsRequest.newBuilder()
                .setContext(request.getContext())
                .setPremium(Premium.newBuilder()
                        .setCard(PremiumCardSettlement.newBuilder()
                                .setNetwork(switch (request.getLoanApplication().getSettlementCase()) {
                                    case CARD -> request.getLoanApplication().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = insurancePremiumCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculatePremiums(outbound1);

        var outbound2 = ListDocumentsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListDocumentsRequest.Filter.newBuilder()
                        .putAllLabels(reply0.getLoan().getLabelsMap())
                        .setRegion(reply1.getPremium().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply2 = RpcSupport.call(outbound2, value -> documentsDocumentServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listDocuments(value));
        log.atDebug().addKeyValue("auditValue", request.getLoanApplication().getLoanApplicationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeLoanApplication", request.getLoanApplication().getLoanApplicationId());
        return NormalizeLoanApplicationResponse.newBuilder()
                .setLoanApplication(LoanApplication.newBuilder()
                        .setDueAt(request.getLoanApplication().getDueAt())
                        .setLoanApplicationId(request.getLoanApplicationId())
                        .build())
                .setOutcome(NormalizeLoanApplicationResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Explains a loan application.
     */
    public ExplainLoanApplicationDecisionResponse explainLoanApplicationDecision(ExplainLoanApplicationDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainLoanApplicationDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoanApplication(request.getContext().getTenantId(), request.getLoanApplication());

        var outbound0 = PullAuditTrailsRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getLoanApplication().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PullAuditTrailsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> auditAuditTrailSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pullAuditTrails(outbound0), rpcExecutor);

        var outbound1 = ExplainLoanCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setLoan(Loan.newBuilder()
                        .setCard(LoanCardFunding.newBuilder()
                                .setToken(request.getLoanApplication().getCard().getToken())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> lendingLoanCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainLoanCalculation(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getLoanApplication().getLoanApplicationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainLoanApplicationDecision", request.getLoanApplication().getLoanApplicationId());
        return ExplainLoanApplicationDecisionResponse.newBuilder()
                .setLoanApplication(LoanApplication.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply0.getAuditTrail().getBillingAddress().getRegion())
                                .build())
                        .setDueAt(request.getLoanApplication().getDueAt())
                        .setLoanApplicationId(request.getLoanApplicationId())
                        .build())
                .setOutcome(ExplainLoanApplicationDecisionResponse.Outcome.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .addAllNextSteps(reply1.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Lints a loan application.
     */
    public LintLoanApplicationsResponse lintLoanApplications(LintLoanApplicationsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintLoanApplications")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoanApplication(request.getContext().getTenantId(), request.getLoanApplication());

        var outbound0 = BeanCopies.copy(EstimateLoanApplicationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EstimateLoanApplicationRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(request.getLoanApplication().getBillingAddress().getPostalCode())
                                .setRecipient(request.getLoanApplication().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getLoanApplication().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getLoanApplication().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build(), EstimateLoanApplicationRequest.class);
        var reply0 = lendingLoanApplicationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateLoanApplication(outbound0);

        var outbound1 = (FindUsersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindUsersRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getLoanApplication().getIssuedAt())
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setRegion(request.getLoanApplication().getBillingAddress().getRegion())
                        .build())
                .build()).toBuilder().build();
        var reply1 = identityUserQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findUsers(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getLoanApplication().getLoanApplicationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintLoanApplications", request.getLoanApplication().getLoanApplicationId());
        return LintLoanApplicationsResponse.newBuilder()
                .setLoanApplication(LoanApplication.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply0.getLoanApplication().getBillingAddress().getRegion())
                                .build())
                        .setDueAt(reply0.getLoanApplication().getDueAt())
                        .setLoanApplicationId(request.getLoanApplicationId())
                        .setStatus(request.getLoanApplication().getStatus())
                        .build())
                .setOutcome(LintLoanApplicationsResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a loan application.
     */
    public GetLoanApplicationRulesResponse getLoanApplicationRules(GetLoanApplicationRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetLoanApplicationRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LoanApplication", request.getLoanApplicationId(), "GetLoanApplicationRules");

        var outbound0 = RecalculateLoansRequest.newBuilder()
                .setContext(request.getContext())
                .setLoan(Loan.newBuilder()
                        .setHolder(LoanApplicationRef.newBuilder()
                                .setLoanApplicationId(request.getLoanApplicationId())
                                .build())
                        .build())
                .build();
        var reply0 = lendingLoanCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateLoans(outbound0);
        return GetLoanApplicationRulesResponse.newBuilder()
                .setLoanApplication(LoanApplication.newBuilder()
                        .setLoanApplicationId(request.getLoanApplicationId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
