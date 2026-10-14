package com.acme.finance.domains.collections;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailCardSettlement;
import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.audit.v1.CheckAuditTrailEligibilityRequest;
import com.acme.audit.v1.CheckAuditTrailEligibilityResponse;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.BatchGetAccessPoliciesRequest;
import com.acme.authorization.v1.BatchGetAccessPoliciesResponse;
import com.acme.authorization.v1.CountAccessPoliciesRequest;
import com.acme.authorization.v1.CountAccessPoliciesResponse;
import com.acme.banking.v1.CountStatementsRequest;
import com.acme.banking.v1.CountStatementsResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.banking.v1.MandateValidationServiceGrpc;
import com.acme.banking.v1.PreviewMandateRequest;
import com.acme.banking.v1.PreviewMandateResponse;
import com.acme.banking.v1.QuoteMandateRequest;
import com.acme.banking.v1.QuoteMandateResponse;
import com.acme.banking.v1.StatementQueryServiceGrpc;
import com.acme.boxoffice.v1.BoxOfficeSaleQueryServiceGrpc;
import com.acme.boxoffice.v1.FindBoxOfficeSalesRequest;
import com.acme.boxoffice.v1.FindBoxOfficeSalesResponse;
import com.acme.collections.v1.CheckWriteOffEligibilityRequest;
import com.acme.collections.v1.CheckWriteOffEligibilityResponse;
import com.acme.collections.v1.CollectionCaseRef;
import com.acme.collections.v1.ExplainWriteOffDecisionRequest;
import com.acme.collections.v1.ExplainWriteOffDecisionResponse;
import com.acme.collections.v1.GetWriteOffRulesRequest;
import com.acme.collections.v1.GetWriteOffRulesResponse;
import com.acme.collections.v1.LintWriteOffsRequest;
import com.acme.collections.v1.LintWriteOffsResponse;
import com.acme.collections.v1.NormalizeWriteOffRequest;
import com.acme.collections.v1.NormalizeWriteOffResponse;
import com.acme.collections.v1.PreviewWriteOffRequest;
import com.acme.collections.v1.PreviewWriteOffResponse;
import com.acme.collections.v1.ScoreWriteOffRequest;
import com.acme.collections.v1.ScoreWriteOffResponse;
import com.acme.collections.v1.ValidateWriteOffRequest;
import com.acme.collections.v1.ValidateWriteOffResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.documents.v1.CountDocumentsRequest;
import com.acme.documents.v1.CountDocumentsResponse;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentBankSettlement;
import com.acme.documents.v1.DocumentQueryServiceGrpc;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.documents.v1.PreviewDocumentRequest;
import com.acme.documents.v1.PreviewDocumentResponse;
import com.acme.documents.v1.SuggestDocumentsRequest;
import com.acme.documents.v1.SuggestDocumentsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CalculateCoverageRequest;
import com.acme.insurance.v1.CalculateCoverageResponse;
import com.acme.insurance.v1.CheckUnderwritingEligibilityRequest;
import com.acme.insurance.v1.CheckUnderwritingEligibilityResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimValidationServiceGrpc;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.CoverageValidationServiceGrpc;
import com.acme.insurance.v1.NormalizeClaimRequest;
import com.acme.insurance.v1.NormalizeClaimResponse;
import com.acme.insurance.v1.NormalizeCoverageRequest;
import com.acme.insurance.v1.NormalizeCoverageResponse;
import com.acme.insurance.v1.PolicyQueryServiceGrpc;
import com.acme.insurance.v1.SummarizePoliciesRequest;
import com.acme.insurance.v1.SummarizePoliciesResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingValidationServiceGrpc;
import com.acme.pipelines.v1.ExportPipelinesRequest;
import com.acme.pipelines.v1.ExportPipelinesResponse;
import com.acme.pipelines.v1.FindPipelinesRequest;
import com.acme.pipelines.v1.FindPipelinesResponse;
import com.acme.pipelines.v1.PipelineQueryServiceGrpc;
import com.acme.search.v1.SearchIndexServiceGrpc;
import com.acme.search.v1.SearchSearchIndexesRequest;
import com.acme.search.v1.SearchSearchIndexesResponse;
import com.acme.sponsorships.v1.SimulateSponsorRequest;
import com.acme.sponsorships.v1.SimulateSponsorResponse;
import com.acme.sponsorships.v1.Sponsor;
import com.acme.sponsorships.v1.SponsorCalculationServiceGrpc;
import com.acme.venues.v1.CreateVenueRequest;
import com.acme.venues.v1.CreateVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
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
 * Checks write offs against the rules before anything acts on them. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class WriteOffValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(WriteOffValidationApplication.class);

    private final AuditTrailValidationServiceGrpc.AuditTrailValidationServiceBlockingStub auditAuditTrailValidationServiceBlocking;

    private final AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking;

    private final AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking;

    private final MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking;

    private final MandateValidationServiceGrpc.MandateValidationServiceBlockingStub bankingMandateValidationServiceBlocking;

    private final StatementQueryServiceGrpc.StatementQueryServiceBlockingStub bankingStatementQueryServiceBlocking;

    private final BoxOfficeSaleQueryServiceGrpc.BoxOfficeSaleQueryServiceBlockingStub boxofficeBoxOfficeSaleQueryServiceBlocking;

    private final ChannelRegistry channels;

    private final DocumentQueryServiceGrpc.DocumentQueryServiceBlockingStub documentsDocumentQueryServiceBlocking;

    private final DocumentValidationServiceGrpc.DocumentValidationServiceBlockingStub documentsDocumentValidationServiceBlocking;

    private final ClaimValidationServiceGrpc.ClaimValidationServiceBlockingStub insuranceClaimValidationServiceBlocking;

    private final CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking;

    private final CoverageValidationServiceGrpc.CoverageValidationServiceBlockingStub insuranceCoverageValidationServiceBlocking;

    private final PolicyQueryServiceGrpc.PolicyQueryServiceBlockingStub insurancePolicyQueryServiceBlocking;

    private final UnderwritingValidationServiceGrpc.UnderwritingValidationServiceBlockingStub insuranceUnderwritingValidationServiceBlocking;

    private final WriteOffValidationRequestMapper mapper;

    private final PipelineQueryServiceGrpc.PipelineQueryServiceBlockingStub pipelinesPipelineQueryServiceBlocking;

    private final PipelineQueryServiceGrpc.PipelineQueryServiceFutureStub pipelinesPipelineQueryServiceFuture;

    private final CollectionsRepository repository;

    private final Executor rpcExecutor;

    private final SearchIndexServiceGrpc.SearchIndexServiceBlockingStub searchSearchIndexServiceBlocking;

    private final SponsorCalculationServiceGrpc.SponsorCalculationServiceBlockingStub sponsorshipsSponsorCalculationServiceBlocking;

    private final VenueServiceGrpc.VenueServiceBlockingStub venuesVenueServiceBlocking;

    public WriteOffValidationApplication(
            AuditTrailValidationServiceGrpc.AuditTrailValidationServiceBlockingStub auditAuditTrailValidationServiceBlocking,
            AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking,
            AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking,
            MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking,
            MandateValidationServiceGrpc.MandateValidationServiceBlockingStub bankingMandateValidationServiceBlocking,
            StatementQueryServiceGrpc.StatementQueryServiceBlockingStub bankingStatementQueryServiceBlocking,
            BoxOfficeSaleQueryServiceGrpc.BoxOfficeSaleQueryServiceBlockingStub boxofficeBoxOfficeSaleQueryServiceBlocking,
            ChannelRegistry channels,
            DocumentQueryServiceGrpc.DocumentQueryServiceBlockingStub documentsDocumentQueryServiceBlocking,
            DocumentValidationServiceGrpc.DocumentValidationServiceBlockingStub documentsDocumentValidationServiceBlocking,
            ClaimValidationServiceGrpc.ClaimValidationServiceBlockingStub insuranceClaimValidationServiceBlocking,
            CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking,
            CoverageValidationServiceGrpc.CoverageValidationServiceBlockingStub insuranceCoverageValidationServiceBlocking,
            PolicyQueryServiceGrpc.PolicyQueryServiceBlockingStub insurancePolicyQueryServiceBlocking,
            UnderwritingValidationServiceGrpc.UnderwritingValidationServiceBlockingStub insuranceUnderwritingValidationServiceBlocking,
            WriteOffValidationRequestMapper mapper,
            PipelineQueryServiceGrpc.PipelineQueryServiceBlockingStub pipelinesPipelineQueryServiceBlocking,
            PipelineQueryServiceGrpc.PipelineQueryServiceFutureStub pipelinesPipelineQueryServiceFuture,
            CollectionsRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SearchIndexServiceGrpc.SearchIndexServiceBlockingStub searchSearchIndexServiceBlocking,
            SponsorCalculationServiceGrpc.SponsorCalculationServiceBlockingStub sponsorshipsSponsorCalculationServiceBlocking,
            VenueServiceGrpc.VenueServiceBlockingStub venuesVenueServiceBlocking) {
        this.auditAuditTrailValidationServiceBlocking = auditAuditTrailValidationServiceBlocking;
        this.authorizationAccessPolicyQueryServiceBlocking = authorizationAccessPolicyQueryServiceBlocking;
        this.authorizationAccessPolicyServiceBlocking = authorizationAccessPolicyServiceBlocking;
        this.bankingMandateCalculationServiceBlocking = bankingMandateCalculationServiceBlocking;
        this.bankingMandateValidationServiceBlocking = bankingMandateValidationServiceBlocking;
        this.bankingStatementQueryServiceBlocking = bankingStatementQueryServiceBlocking;
        this.boxofficeBoxOfficeSaleQueryServiceBlocking = boxofficeBoxOfficeSaleQueryServiceBlocking;
        this.channels = channels;
        this.documentsDocumentQueryServiceBlocking = documentsDocumentQueryServiceBlocking;
        this.documentsDocumentValidationServiceBlocking = documentsDocumentValidationServiceBlocking;
        this.insuranceClaimValidationServiceBlocking = insuranceClaimValidationServiceBlocking;
        this.insuranceCoverageCalculationServiceBlocking = insuranceCoverageCalculationServiceBlocking;
        this.insuranceCoverageValidationServiceBlocking = insuranceCoverageValidationServiceBlocking;
        this.insurancePolicyQueryServiceBlocking = insurancePolicyQueryServiceBlocking;
        this.insuranceUnderwritingValidationServiceBlocking = insuranceUnderwritingValidationServiceBlocking;
        this.mapper = mapper;
        this.pipelinesPipelineQueryServiceBlocking = pipelinesPipelineQueryServiceBlocking;
        this.pipelinesPipelineQueryServiceFuture = pipelinesPipelineQueryServiceFuture;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.searchSearchIndexServiceBlocking = searchSearchIndexServiceBlocking;
        this.sponsorshipsSponsorCalculationServiceBlocking = sponsorshipsSponsorCalculationServiceBlocking;
        this.venuesVenueServiceBlocking = venuesVenueServiceBlocking;
    }

    /**
     * Validates a write off.
     */
    public ValidateWriteOffResponse validateWriteOff(ValidateWriteOffRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateWriteOff")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveWriteOff(request.getContext().getTenantId(), request.getWriteOff());

        var outbound0 = SummarizePoliciesRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SummarizePoliciesRequest.Details.newBuilder()
                        .putAllFilters(request.getWriteOff().getMetadataMap())
                        .build())
                .setReason(request.getReason())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(insurancePolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizePolicies(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = CheckAuditTrailEligibilityRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getWriteOff().getAudit().getCreatedBy())
                                .setUpdatedAt(request.getWriteOff().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(request.getWriteOff().getBillingAddress().getLocality())
                                .build())
                        .setCard(AuditTrailCardSettlement.newBuilder()
                                .setToken(switch (request.getWriteOff().getSettlementCase()) {
                                    case CARD -> request.getWriteOff().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = auditAuditTrailValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkAuditTrailEligibility(outbound1);

        var outbound2 = RpcSupport.unpack(Any.pack(CountDocumentsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountDocumentsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply1.getAuditTrail().getAudit().getUpdatedAt())
                        .setOwnerId(request.getWriteOff().getCounterparty().getCollectionCaseId())
                        .setRegion(reply1.getAuditTrail().getBillingAddress().getRegion())
                        .build())
                .build()), CountDocumentsRequest.class);
        var reply2 = documentsDocumentQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countDocuments(outbound2);

        var outbound3 = CreateVenueRequest.newBuilder()
                .setContext(request.getContext())
                .setVenue(Venue.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setLocality(request.getWriteOff().getBillingAddress().getLocality())
                                .build())
                        .build())
                .build();
        Function<CreateVenueRequest, CreateVenueResponse> operation3 = venuesVenueServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::createVenue;
        var reply3 = operation3.apply(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getWriteOff().getWriteOffId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateWriteOff", request.getWriteOff().getWriteOffId());
        return ValidateWriteOffResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .setWriteOff(WriteOff.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply3.getVenue().getAddress().getLinesList())
                                .build())
                        .setCounterparty(CollectionCaseRef.newBuilder()
                                .setDisplayName(reply3.getVenue().getOwner().getDisplayName())
                                .build())
                        .setIssuedAt(reply1.getAuditTrail().getIssuedAt())
                        .putAllMetadata(request.getWriteOff().getMetadataMap())
                        .setWriteOffId(request.getWriteOffId())
                        .build())
                .build();
    }

    /**
     * Previews a write off.
     */
    public PreviewWriteOffResponse previewWriteOff(PreviewWriteOffRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewWriteOff")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveWriteOff(request.getContext().getTenantId(), request.getWriteOff());

        var outbound0 = FindPipelinesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindPipelinesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getWriteOff().getIssuedAt())
                        .setRegion(request.getWriteOff().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending0 = pipelinesPipelineQueryServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).findPipelines(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = NormalizeCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeCoverageRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .build();
        Function<NormalizeCoverageRequest, NormalizeCoverageResponse> operation1 = insuranceCoverageValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::normalizeCoverage;
        var reply1 = operation1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getWriteOff().getWriteOffId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewWriteOff", request.getWriteOff().getWriteOffId());
        return PreviewWriteOffResponse.newBuilder()
                .setOutcome(PreviewWriteOffResponse.Outcome.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .setWriteOff(WriteOff.newBuilder()
                        .setNotes(request.getWriteOff().getNotes())
                        .setWriteOffId(request.getWriteOffId())
                        .build())
                .build();
    }

    /**
     * Checks a write off.
     */
    public CheckWriteOffEligibilityResponse checkWriteOffEligibility(CheckWriteOffEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckWriteOffEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveWriteOff(request.getContext().getTenantId(), request.getWriteOff());

        var outbound0 = SearchSearchIndexesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchSearchIndexesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getWriteOff().getIssuedAt())
                        .setRegion(request.getWriteOff().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(searchSearchIndexServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchSearchIndexes(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = SimulateSponsorRequest.newBuilder()
                .setContext(request.getContext())
                .setSponsor(Sponsor.newBuilder()
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getWriteOff().getBillingAddress().getCountryCode())
                                .setLocality(request.getWriteOff().getBillingAddress().getLocality())
                                .build())
                        .build())
                .build();
        var reply1 = sponsorshipsSponsorCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateSponsor(outbound1);

        var outbound2 = JsonCodec.roundTrip(FindBoxOfficeSalesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindBoxOfficeSalesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply1.getSponsor().getAudit().getUpdatedAt())
                        .putAllLabels(reply1.getSponsor().getAttributesMap())
                        .setOwnerId(request.getWriteOff().getCounterparty().getCollectionCaseId())
                        .setRegion(reply1.getSponsor().getPrimaryAddress().getRegion())
                        .build())
                .build(), FindBoxOfficeSalesRequest.class);
        var reply2 = boxofficeBoxOfficeSaleQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findBoxOfficeSales(outbound2);

        var outbound3 = mapper.checkWriteOffEligibilityStage3(request, reply1);
        var reply3 = bankingStatementQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countStatements(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getWriteOff().getWriteOffId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckWriteOffEligibility", request.getWriteOff().getWriteOffId());
        return CheckWriteOffEligibilityResponse.newBuilder()
                .setOutcome(CheckWriteOffEligibilityResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setWriteOff(WriteOff.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getSponsor().getAudit().getUpdatedBy())
                                .build())
                        .setStatus(request.getWriteOff().getStatus())
                        .setWriteOffId(request.getWriteOffId())
                        .build())
                .build();
    }

    /**
     * Normalizes a write off.
     */
    public NormalizeWriteOffResponse normalizeWriteOff(NormalizeWriteOffRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeWriteOff")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveWriteOff(request.getContext().getTenantId(), request.getWriteOff());

        var outbound0 = CalculateCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CalculateCoverageRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(request.getWriteOff().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> insuranceCoverageCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateCoverage(outbound0), rpcExecutor);

        var outbound1 = CheckUnderwritingEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckUnderwritingEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getContact().getName())
                                .build())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> insuranceUnderwritingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkUnderwritingEligibility(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = (CountAccessPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountAccessPoliciesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply1.getUnderwriting().getSla().getDueAt())
                        .putAllLabels(request.getWriteOff().getMetadataMap())
                        .setOwnerId(reply0.getCoverage().getCoverageId())
                        .setRegion(request.getWriteOff().getBillingAddress().getRegion())
                        .build())
                .build()).toBuilder().build();
        var reply2 = authorizationAccessPolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countAccessPolicies(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getWriteOff().getWriteOffId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeWriteOff", request.getWriteOff().getWriteOffId());
        return NormalizeWriteOffResponse.newBuilder()
                .setOutcome(NormalizeWriteOffResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .build())
                .setWriteOff(WriteOff.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getCoverage().getAudit().getUpdatedAt())
                                .build())
                        .setNotes(request.getWriteOff().getNotes())
                        .setWriteOffId(request.getWriteOffId())
                        .build())
                .build();
    }

    /**
     * Explains a write off.
     */
    public ExplainWriteOffDecisionResponse explainWriteOffDecision(ExplainWriteOffDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainWriteOffDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveWriteOff(request.getContext().getTenantId(), request.getWriteOff());

        var outbound0 = mapper.explainWriteOffDecisionStage0(request);
        var reply0 = documentsDocumentQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestDocuments(outbound0);

        var outbound1 = PreviewDocumentRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewDocumentRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getContact().getName())
                                .build())
                        .build())
                .setDocument(Document.newBuilder()
                        .setBank(DocumentBankSettlement.newBuilder()
                                .setAccountHolder(switch (request.getWriteOff().getSettlementCase()) {
                                    case BANK -> request.getWriteOff().getBank().getAccountHolder();
                                    default -> "";
                                })
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(request.getWriteOff().getBillingAddress().getLocality())
                                .build())
                        .build())
                .build();
        var reply1 = documentsDocumentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewDocument(outbound1);

        var outbound2 = ExportPipelinesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportPipelinesRequest.Filter.newBuilder()
                        .setRegion(request.getWriteOff().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream2 = pipelinesPipelineQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportPipelines(outbound2);
        var reply2 = ExportPipelinesResponse.getDefaultInstance();
        while (stream2.hasNext()) {
            reply2 = stream2.next();
        }

        var outbound3 = QuoteMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(QuoteMandateRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getWriteOff().getBillingAddress().getLocality())
                                .setPostalCode(reply1.getDocument().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .setMandate(Mandate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getDocument().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getWriteOff().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build();
        var reply3 = bankingMandateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteMandate(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getWriteOff().getWriteOffId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainWriteOffDecision", request.getWriteOff().getWriteOffId());
        return ExplainWriteOffDecisionResponse.newBuilder()
                .setOutcome(ExplainWriteOffDecisionResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .setWriteOff(WriteOff.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getDocument().getBillingAddress().getCountryCode())
                                .build())
                        .putAllMetadata(reply3.getMandate().getMetadataMap())
                        .setStatus(request.getWriteOff().getStatus())
                        .setWriteOffId(request.getWriteOffId())
                        .build())
                .build();
    }

    /**
     * Scores a write off.
     */
    public ScoreWriteOffResponse scoreWriteOff(ScoreWriteOffRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreWriteOff")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveWriteOff(request.getContext().getTenantId(), request.getWriteOff());

        var outbound0 = FindBoxOfficeSalesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindBoxOfficeSalesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getWriteOff().getAudit().getCreatedAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getWriteOff().getCounterparty().getCollectionCaseId())
                        .setRegion(request.getWriteOff().getBillingAddress().getRegion())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.boxoffice.v1.BoxOfficeSaleQueryService");
        var serviceName0 = "acme.boxoffice.v1" + "." + "BoxOfficeSaleQueryService";
        var methodName0 = "Fin" + "dBoxOfficeSales";
        var descriptor0 = MethodDescriptor.<FindBoxOfficeSalesRequest, FindBoxOfficeSalesResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(FindBoxOfficeSalesRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(FindBoxOfficeSalesResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = BatchGetAccessPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        RpcPort<BatchGetAccessPoliciesRequest, BatchGetAccessPoliciesResponse> port1 = RpcPort.adapt(authorizationAccessPolicyServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::batchGetAccessPolicies);
        var reply1 = port1.exchange(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getWriteOff().getWriteOffId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreWriteOff", request.getWriteOff().getWriteOffId());
        return ScoreWriteOffResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .setWriteOff(WriteOff.newBuilder()
                        .setTotal(request.getWriteOff().getTotal())
                        .setWriteOffId(request.getWriteOffId())
                        .build())
                .build();
    }

    /**
     * Lints a write off.
     */
    public LintWriteOffsResponse lintWriteOffs(LintWriteOffsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintWriteOffs")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveWriteOff(request.getContext().getTenantId(), request.getWriteOff());

        var outbound0 = PreviewMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewMandateRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> bankingMandateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewMandate(value)).orElseThrow();

        var outbound1 = NormalizeClaimRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getMandate().getCounterparty().getDisplayName())
                                .build())
                        .setCustomer(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = insuranceClaimValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeClaim(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getWriteOff().getWriteOffId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintWriteOffs", request.getWriteOff().getWriteOffId());
        return LintWriteOffsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setWriteOff(WriteOff.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getMandate().getAudit().getUpdatedAt())
                                .build())
                        .setDueAt(request.getWriteOff().getDueAt())
                        .setWriteOffId(request.getWriteOffId())
                        .build())
                .build();
    }

    /**
     * Returns a write off.
     */
    public GetWriteOffRulesResponse getWriteOffRules(GetWriteOffRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetWriteOffRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "WriteOff", request.getWriteOffId(), "GetWriteOffRules");

        var outbound0 = NormalizeCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setDetails(NormalizeCoverageRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> insuranceCoverageValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeCoverage(value));
        return GetWriteOffRulesResponse.newBuilder()
                .setWriteOff(WriteOff.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getCoverage().getAudit().getCreatedAt())
                                .build())
                        .setWriteOffId(request.getWriteOffId())
                        .build())
                .build();
    }
}
