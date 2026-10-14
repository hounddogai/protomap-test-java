package com.acme.finance.domains.compliance;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.CompareAuditTrailVersionsRequest;
import com.acme.audit.v1.CompareAuditTrailVersionsResponse;
import com.acme.audit.v1.NotifyRetentionPolicyWatchersRequest;
import com.acme.audit.v1.NotifyRetentionPolicyWatchersResponse;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.UnsubscribeRetentionPolicyUpdatesRequest;
import com.acme.audit.v1.UnsubscribeRetentionPolicyUpdatesResponse;
import com.acme.banking.v1.BankAccountQueryServiceGrpc;
import com.acme.banking.v1.SuggestBankAccountsRequest;
import com.acme.banking.v1.SuggestBankAccountsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationLine;
import com.acme.compliance.v1.AttestationLineTax;
import com.acme.compliance.v1.AttestationRef;
import com.acme.compliance.v1.CalculateAttestationRequest;
import com.acme.compliance.v1.CalculateAttestationResponse;
import com.acme.compliance.v1.CalculateComplianceCaseRequest;
import com.acme.compliance.v1.CalculateComplianceCaseResponse;
import com.acme.compliance.v1.CompareAttestationOptionsRequest;
import com.acme.compliance.v1.CompareAttestationOptionsResponse;
import com.acme.compliance.v1.CompareComplianceCaseOptionsRequest;
import com.acme.compliance.v1.CompareComplianceCaseOptionsResponse;
import com.acme.compliance.v1.CompareControlOptionsRequest;
import com.acme.compliance.v1.CompareControlOptionsResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseValidationServiceGrpc;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.compliance.v1.EstimateAttestationRequest;
import com.acme.compliance.v1.EstimateAttestationResponse;
import com.acme.compliance.v1.ExplainAttestationCalculationRequest;
import com.acme.compliance.v1.ExplainAttestationCalculationResponse;
import com.acme.compliance.v1.ExplainComplianceCaseCalculationRequest;
import com.acme.compliance.v1.ExplainComplianceCaseCalculationResponse;
import com.acme.compliance.v1.GetAttestationRatesRequest;
import com.acme.compliance.v1.GetAttestationRatesResponse;
import com.acme.compliance.v1.GetComplianceCaseRatesRequest;
import com.acme.compliance.v1.GetComplianceCaseRatesResponse;
import com.acme.compliance.v1.QuoteAttestationRequest;
import com.acme.compliance.v1.QuoteAttestationResponse;
import com.acme.compliance.v1.RecalculateAttestationsRequest;
import com.acme.compliance.v1.RecalculateAttestationsResponse;
import com.acme.compliance.v1.SimulateAttestationRequest;
import com.acme.compliance.v1.SimulateAttestationResponse;
import com.acme.compliance.v1.ValidateComplianceCaseRequest;
import com.acme.compliance.v1.ValidateComplianceCaseResponse;
import com.acme.documents.v1.CheckDocumentEligibilityRequest;
import com.acme.documents.v1.CheckDocumentEligibilityResponse;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentFolderRef;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.forecasting.v1.DemandForecastQueryServiceGrpc;
import com.acme.forecasting.v1.SuggestDemandForecastsRequest;
import com.acme.forecasting.v1.SuggestDemandForecastsResponse;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ClaimQueryServiceGrpc;
import com.acme.insurance.v1.CountPoliciesRequest;
import com.acme.insurance.v1.CountPoliciesResponse;
import com.acme.insurance.v1.PolicyQueryServiceGrpc;
import com.acme.insurance.v1.SummarizeClaimsRequest;
import com.acme.insurance.v1.SummarizeClaimsResponse;
import com.acme.ledger.v1.BrowseJournalEntriesRequest;
import com.acme.ledger.v1.BrowseJournalEntriesResponse;
import com.acme.ledger.v1.JournalEntryQueryServiceGrpc;
import com.acme.ledger.v1.LedgerAccountQueryServiceGrpc;
import com.acme.ledger.v1.ResolveLedgerAccountsRequest;
import com.acme.ledger.v1.ResolveLedgerAccountsResponse;
import com.acme.risk.v1.ExplainStressTestCalculationRequest;
import com.acme.risk.v1.ExplainStressTestCalculationResponse;
import com.acme.risk.v1.LintRiskRegistersRequest;
import com.acme.risk.v1.LintRiskRegistersResponse;
import com.acme.risk.v1.RecalculateStressTestsRequest;
import com.acme.risk.v1.RecalculateStressTestsResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterValidationServiceGrpc;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestCalculationServiceGrpc;
import com.acme.risk.v1.StressTestLine;
import com.acme.risk.v1.StressTestLineTax;
import com.acme.search.v1.BatchGetSearchIndexesRequest;
import com.acme.search.v1.BatchGetSearchIndexesResponse;
import com.acme.search.v1.SearchIndexServiceGrpc;
import com.acme.webhooks.v1.SummarizeWebhookEndpointsRequest;
import com.acme.webhooks.v1.SummarizeWebhookEndpointsResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.List;
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
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Computes the figures of an attestation, such as totals, taxes, and estimates. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class AttestationCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(AttestationCalculationApplication.class);

    private final RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking;

    private final BankAccountQueryServiceGrpc.BankAccountQueryServiceBlockingStub bankingBankAccountQueryServiceBlocking;

    private final ChannelRegistry channels;

    private final ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceBlockingStub complianceComplianceCaseCalculationServiceBlocking;

    private final ComplianceCaseValidationServiceGrpc.ComplianceCaseValidationServiceBlockingStub complianceComplianceCaseValidationServiceBlocking;

    private final ControlCalculationServiceGrpc.ControlCalculationServiceBlockingStub complianceControlCalculationServiceBlocking;

    private final DocumentValidationServiceGrpc.DocumentValidationServiceBlockingStub documentsDocumentValidationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final DemandForecastQueryServiceGrpc.DemandForecastQueryServiceBlockingStub forecastingDemandForecastQueryServiceBlocking;

    private final ClaimQueryServiceGrpc.ClaimQueryServiceBlockingStub insuranceClaimQueryServiceBlocking;

    private final PolicyQueryServiceGrpc.PolicyQueryServiceBlockingStub insurancePolicyQueryServiceBlocking;

    private final JournalEntryQueryServiceGrpc.JournalEntryQueryServiceFutureStub ledgerJournalEntryQueryServiceFuture;

    private final LedgerAccountQueryServiceGrpc.LedgerAccountQueryServiceBlockingStub ledgerLedgerAccountQueryServiceBlocking;

    private final AttestationCalculationRequestMapper mapper;

    private final ComplianceRepository repository;

    private final RiskRegisterValidationServiceGrpc.RiskRegisterValidationServiceBlockingStub riskRiskRegisterValidationServiceBlocking;

    private final StressTestCalculationServiceGrpc.StressTestCalculationServiceBlockingStub riskStressTestCalculationServiceBlocking;

    private final Executor rpcExecutor;

    private final SearchIndexServiceGrpc.SearchIndexServiceBlockingStub searchSearchIndexServiceBlocking;

    public AttestationCalculationApplication(
            RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking,
            BankAccountQueryServiceGrpc.BankAccountQueryServiceBlockingStub bankingBankAccountQueryServiceBlocking,
            ChannelRegistry channels,
            ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceBlockingStub complianceComplianceCaseCalculationServiceBlocking,
            ComplianceCaseValidationServiceGrpc.ComplianceCaseValidationServiceBlockingStub complianceComplianceCaseValidationServiceBlocking,
            ControlCalculationServiceGrpc.ControlCalculationServiceBlockingStub complianceControlCalculationServiceBlocking,
            DocumentValidationServiceGrpc.DocumentValidationServiceBlockingStub documentsDocumentValidationServiceBlocking,
            ApplicationEventPublisher events,
            DemandForecastQueryServiceGrpc.DemandForecastQueryServiceBlockingStub forecastingDemandForecastQueryServiceBlocking,
            ClaimQueryServiceGrpc.ClaimQueryServiceBlockingStub insuranceClaimQueryServiceBlocking,
            PolicyQueryServiceGrpc.PolicyQueryServiceBlockingStub insurancePolicyQueryServiceBlocking,
            JournalEntryQueryServiceGrpc.JournalEntryQueryServiceFutureStub ledgerJournalEntryQueryServiceFuture,
            LedgerAccountQueryServiceGrpc.LedgerAccountQueryServiceBlockingStub ledgerLedgerAccountQueryServiceBlocking,
            AttestationCalculationRequestMapper mapper,
            ComplianceRepository repository,
            RiskRegisterValidationServiceGrpc.RiskRegisterValidationServiceBlockingStub riskRiskRegisterValidationServiceBlocking,
            StressTestCalculationServiceGrpc.StressTestCalculationServiceBlockingStub riskStressTestCalculationServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SearchIndexServiceGrpc.SearchIndexServiceBlockingStub searchSearchIndexServiceBlocking) {
        this.auditRetentionPolicyNotificationServiceBlocking = auditRetentionPolicyNotificationServiceBlocking;
        this.bankingBankAccountQueryServiceBlocking = bankingBankAccountQueryServiceBlocking;
        this.channels = channels;
        this.complianceComplianceCaseCalculationServiceBlocking = complianceComplianceCaseCalculationServiceBlocking;
        this.complianceComplianceCaseValidationServiceBlocking = complianceComplianceCaseValidationServiceBlocking;
        this.complianceControlCalculationServiceBlocking = complianceControlCalculationServiceBlocking;
        this.documentsDocumentValidationServiceBlocking = documentsDocumentValidationServiceBlocking;
        this.events = events;
        this.forecastingDemandForecastQueryServiceBlocking = forecastingDemandForecastQueryServiceBlocking;
        this.insuranceClaimQueryServiceBlocking = insuranceClaimQueryServiceBlocking;
        this.insurancePolicyQueryServiceBlocking = insurancePolicyQueryServiceBlocking;
        this.ledgerJournalEntryQueryServiceFuture = ledgerJournalEntryQueryServiceFuture;
        this.ledgerLedgerAccountQueryServiceBlocking = ledgerLedgerAccountQueryServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.riskRiskRegisterValidationServiceBlocking = riskRiskRegisterValidationServiceBlocking;
        this.riskStressTestCalculationServiceBlocking = riskStressTestCalculationServiceBlocking;
        this.rpcExecutor = rpcExecutor;
        this.searchSearchIndexServiceBlocking = searchSearchIndexServiceBlocking;
    }

    public record EstimateAttestationStage1Requested(CompareAuditTrailVersionsRequest request, CompletableFuture<CompareAuditTrailVersionsResponse> completion, io.grpc.Context context) {}

    /**
     * Calculates an attestation.
     */
    public CalculateAttestationResponse calculateAttestation(CalculateAttestationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateAttestation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAttestation(request.getContext().getTenantId(), request.getAttestation());

        var outbound0 = CompareControlOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getAttestation().getAudit().getCreatedAt())
                                .setUpdatedAt(request.getAttestation().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setDetails(CompareControlOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(request.getAttestation().getBillingAddress().getPostalCode())
                                .setRegion(request.getDetails().getDestination().getRegion())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> complianceControlCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareControlOptions(outbound0), rpcExecutor);

        var outbound1 = LintRiskRegistersRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> riskRiskRegisterValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintRiskRegisters(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getAttestation().getAttestationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateAttestation", request.getAttestation().getAttestationId());
        return CalculateAttestationResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(request.getAttestationId())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getRiskRegister().getAudit().getCreatedBy())
                                .setUpdatedBy(reply0.getControl().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(request.getAttestation().getBillingAddress())
                        .addLines(AttestationLine.newBuilder()
                                .putAllAttributes(reply1.getRiskRegister().getAttributesMap())
                                .build())
                        .build())
                .build();
    }

    /**
     * Estimates an attestation.
     */
    public EstimateAttestationResponse estimateAttestation(EstimateAttestationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateAttestation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAttestation(request.getContext().getTenantId(), request.getAttestation());

        var outbound0 = SuggestBankAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestBankAccountsRequest.Filter.newBuilder()
                        .setRegion(request.getAttestation().getBillingAddress().getRegion())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.banking.v1.BankAccountQueryService/SuggestBankAccounts", outbound0);
        var reply0 = bankingBankAccountQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestBankAccounts(command0.payload());

        var outbound1 = CompareAuditTrailVersionsRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getAttestation().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getDetails().getDestination().getPostalCode())
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CompareAuditTrailVersionsRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var completion1 = new CompletableFuture<CompareAuditTrailVersionsResponse>();
        events.publishEvent(new EstimateAttestationStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getAttestation().getAttestationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateAttestation", request.getAttestation().getAttestationId());
        return EstimateAttestationResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(request.getAttestationId())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getAuditTrail().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(request.getAttestation().getBillingAddress())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Quotes an attestation.
     */
    public QuoteAttestationResponse quoteAttestation(QuoteAttestationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuoteAttestation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAttestation(request.getContext().getTenantId(), request.getAttestation());

        var outbound0 = GetComplianceCaseRatesRequest.newBuilder()
                .setComplianceCaseId(request.getAttestation().getCounterparty().getComplianceCaseId())
                .setContext(request.getContext())
                .build();
        final class Stage0Adapter {
            GetComplianceCaseRatesResponse fetch(GetComplianceCaseRatesRequest input) {
                return complianceComplianceCaseCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getComplianceCaseRates(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = BatchGetSearchIndexesRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> searchSearchIndexServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetSearchIndexes(value)).orElseThrow();

        var outbound2 = (UnsubscribeRetentionPolicyUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(UnsubscribeRetentionPolicyUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(reply0.getComplianceCase().getRequester().getEmail())
                                .setPreferredChannel(reply0.getComplianceCase().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply2 = auditRetentionPolicyNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).unsubscribeRetentionPolicyUpdates(outbound2);

        var outbound3 = CheckDocumentEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckDocumentEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getComplianceCase().getRequester().getEmail())
                                .setName(reply0.getComplianceCase().getRequester().getName())
                                .build())
                        .build())
                .setDocument(Document.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getAttestation().getBillingAddress().getLinesList())
                                .build())
                        .setCounterparty(DocumentFolderRef.newBuilder()
                                .setDisplayName(switch (reply0.getComplianceCase().getOriginCase()) {
                                    case CUSTOMER -> reply0.getComplianceCase().getCustomer().getDisplayName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply3 = documentsDocumentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkDocumentEligibility(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getAttestation().getAttestationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuoteAttestation", request.getAttestation().getAttestationId());
        return QuoteAttestationResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(request.getAttestationId())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply3.getDocument().getBillingAddress().getRecipient())
                                .build())
                        .setIssuedAt(request.getAttestation().getIssuedAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Compares an attestation.
     */
    public CompareAttestationOptionsResponse compareAttestationOptions(CompareAttestationOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareAttestationOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAttestation(request.getContext().getTenantId(), request.getAttestation());

        var outbound0 = mapper.compareAttestationOptionsStage0(request);
        var reply0 = insuranceClaimQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeClaims(outbound0);

        var outbound1 = JsonCodec.roundTrip(ExplainComplianceCaseCalculationRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getAttestation().getCounterparty().getDisplayName())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getAttestation().getAudit().getEtag())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build(), ExplainComplianceCaseCalculationRequest.class);
        var reply1 = complianceComplianceCaseCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainComplianceCaseCalculation(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getAttestation().getAttestationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareAttestationOptions", request.getAttestation().getAttestationId());
        return CompareAttestationOptionsResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(request.getAttestationId())
                        .setDueAt(reply1.getComplianceCase().getSla().getDueAt())
                        .putAllMetadata(request.getAttestation().getMetadataMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Recalculates an attestation.
     */
    public RecalculateAttestationsResponse recalculateAttestations(RecalculateAttestationsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateAttestations")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAttestation(request.getContext().getTenantId(), request.getAttestation());

        var outbound0 = CalculateComplianceCaseRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getAttestation().getAudit().getCreatedAt())
                                .setCreatedBy(request.getAttestation().getAudit().getCreatedBy())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel0 = channels.lookup("acme.compliance.v1.ComplianceCaseCalculationService");
        var selected0 = ComplianceCaseCalculationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.calculateComplianceCase(outbound0);

        var outbound1 = (NotifyRetentionPolicyWatchersRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NotifyRetentionPolicyWatchersRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setName(reply0.getComplianceCase().getRequester().getName())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply1 = auditRetentionPolicyNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).notifyRetentionPolicyWatchers(outbound1);

        var outbound2 = ValidateComplianceCaseRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getRetentionPolicy().getAudit().getCreatedAt())
                                .build())
                        .setCustomer(AttestationRef.newBuilder()
                                .setDisplayName(reply1.getRetentionPolicy().getOwner().getDisplayName())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setName(reply0.getComplianceCase().getRequester().getName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var command2 = new DispatchCommand<>("acme.compliance.v1.ComplianceCaseValidationService/ValidateComplianceCase", outbound2);
        var reply2 = complianceComplianceCaseValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateComplianceCase(command2.payload());

        var outbound3 = SummarizeWebhookEndpointsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .build();
        var channel3 = channels.lookup("acme.webhooks.v1.WebhookEndpointQueryService");
        var serviceName3 = "acme.webhooks.v1" + "." + "WebhookEndpointQueryService";
        var methodName3 = "Sum" + "marizeWebhookEndpoints";
        var descriptor3 = MethodDescriptor.<SummarizeWebhookEndpointsRequest, SummarizeWebhookEndpointsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName3, methodName3))
                .setRequestMarshaller(ProtoUtils.marshaller(SummarizeWebhookEndpointsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(SummarizeWebhookEndpointsResponse.getDefaultInstance()))
                .build();
        var reply3 = ClientCalls.blockingUnaryCall(channel3, descriptor3,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound3);
        log.atDebug().addKeyValue("auditValue", request.getAttestation().getAttestationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateAttestations", request.getAttestation().getAttestationId());
        return RecalculateAttestationsResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(request.getAttestationId())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getRetentionPolicy().getAudit().getCreatedBy())
                                .build())
                        .setDueAt(reply0.getComplianceCase().getSla().getDueAt())
                        .addLines(AttestationLine.newBuilder()
                                .setTax(AttestationLineTax.newBuilder()
                                        .setAmount(reply1.getRetentionPolicy().getDiscount().getAmount())
                                        .build())
                                .build())
                        .setNumber(reply2.getComplianceCase().getCustomer().getNumber())
                        .setTotal(request.getAttestation().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply3.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Simulates an attestation.
     */
    public SimulateAttestationResponse simulateAttestation(SimulateAttestationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateAttestation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAttestation(request.getContext().getTenantId(), request.getAttestation());

        List<ExplainStressTestCalculationResponse> results0 = request.getAttestation().getLinesList().stream()
                .map(element -> ExplainStressTestCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setStressTest(StressTest.newBuilder()
                        .addLines(StressTestLine.newBuilder()
                                .setLineId(element.getLineId())
                                .setTax(StressTestLineTax.newBuilder()
                                        .setAmount(element.getAmount())
                                        .build())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(riskStressTestCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainStressTestCalculation(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? ExplainStressTestCalculationResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = ResolveLedgerAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        Function<ResolveLedgerAccountsRequest, ResolveLedgerAccountsResponse> operation1 = ledgerLedgerAccountQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::resolveLedgerAccounts;
        var reply1 = operation1.apply(outbound1);

        var outbound2 = mapper.simulateAttestationStage2(request, reply0);
        var reply2 = complianceComplianceCaseCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareComplianceCaseOptions(outbound2);

        var outbound3 = BrowseJournalEntriesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseJournalEntriesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply2.getComplianceCase().getAudit().getUpdatedAt())
                        .setRegion(reply0.getStressTest().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending3 = ledgerJournalEntryQueryServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).browseJournalEntries(outbound3);
        var reply3 = RpcSupport.await(pending3);
        log.atDebug().addKeyValue("auditValue", request.getAttestation().getAttestationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateAttestation", request.getAttestation().getAttestationId());
        return SimulateAttestationResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(request.getAttestationId())
                        .setNumber(request.getAttestation().getNumber())
                        .build())
                .setBreakdown(SimulateAttestationResponse.Breakdown.newBuilder()
                        .setCurrencyCode(reply2.getBreakdown().getCurrencyCode())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply3.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Explains an attestation.
     */
    public ExplainAttestationCalculationResponse explainAttestationCalculation(ExplainAttestationCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainAttestationCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAttestation(request.getContext().getTenantId(), request.getAttestation());

        var outbound0 = SuggestDemandForecastsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestDemandForecastsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getAttestation().getAudit().getUpdatedAt())
                        .setOwnerId(request.getAttestation().getCounterparty().getComplianceCaseId())
                        .setRegion(request.getAttestation().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> forecastingDemandForecastQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestDemandForecasts(outbound0), rpcExecutor);

        var outbound1 = RecalculateStressTestsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecalculateStressTestsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(request.getAttestation().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> riskStressTestCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateStressTests(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getAttestation().getAttestationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainAttestationCalculation", request.getAttestation().getAttestationId());
        return ExplainAttestationCalculationResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(request.getAttestationId())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getStressTest().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply1.getStressTest().getBillingAddress().getLinesList())
                                .build())
                        .setNotes(request.getAttestation().getNotes())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Returns an attestation.
     */
    public GetAttestationRatesResponse getAttestationRates(GetAttestationRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetAttestationRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Attestation", request.getAttestationId(), "GetAttestationRates");

        var outbound0 = CountPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountPoliciesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .putAllLabels(request.getContext().getBaggageMap())
                        .setOwnerId(request.getContext().getActor().getUserId())
                        .build())
                .build();
        final class Stage0Adapter {
            CountPoliciesResponse fetch(CountPoliciesRequest input) {
                return insurancePolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countPolicies(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        return GetAttestationRatesResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(request.getAttestationId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }
}
