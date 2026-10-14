package com.acme.finance.domains.treasury;

import com.acme.audiences.v1.AudienceQueryServiceGrpc;
import com.acme.audiences.v1.ResolveAudiencesRequest;
import com.acme.audiences.v1.ResolveAudiencesResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.AuditTrailQueryServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.SuggestAuditTrailsRequest;
import com.acme.audit.v1.SuggestAuditTrailsResponse;
import com.acme.audit.v1.UpdateAuditEventRequest;
import com.acme.audit.v1.UpdateAuditEventResponse;
import com.acme.banking.v1.CalculateMandateRequest;
import com.acme.banking.v1.CalculateMandateResponse;
import com.acme.banking.v1.ExportStatementsRequest;
import com.acme.banking.v1.ExportStatementsResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateAttachment;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.banking.v1.StatementQueryServiceGrpc;
import com.acme.budgeting.v1.ExplainVarianceDecisionRequest;
import com.acme.budgeting.v1.ExplainVarianceDecisionResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyBankFunding;
import com.acme.insurance.v1.PolicyValidationServiceGrpc;
import com.acme.insurance.v1.ValidatePolicyRequest;
import com.acme.insurance.v1.ValidatePolicyResponse;
import com.acme.search.v1.ResolveSearchIndexesRequest;
import com.acme.search.v1.ResolveSearchIndexesResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferBankSettlement;
import com.acme.treasury.v1.BankTransferCalculationServiceGrpc;
import com.acme.treasury.v1.BankTransferCardSettlement;
import com.acme.treasury.v1.BankTransferLine;
import com.acme.treasury.v1.BankTransferLineTax;
import com.acme.treasury.v1.BankTransferValidationServiceGrpc;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionCalculationServiceGrpc;
import com.acme.treasury.v1.CashPositionCardFunding;
import com.acme.treasury.v1.CashPositionRef;
import com.acme.treasury.v1.CheckFxHedgeEligibilityRequest;
import com.acme.treasury.v1.CheckFxHedgeEligibilityResponse;
import com.acme.treasury.v1.CompareBankTransferOptionsRequest;
import com.acme.treasury.v1.CompareBankTransferOptionsResponse;
import com.acme.treasury.v1.CompareCashPositionOptionsRequest;
import com.acme.treasury.v1.CompareCashPositionOptionsResponse;
import com.acme.treasury.v1.ExplainFxHedgeDecisionRequest;
import com.acme.treasury.v1.ExplainFxHedgeDecisionResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeCalculationServiceGrpc;
import com.acme.treasury.v1.GetFxHedgeRatesRequest;
import com.acme.treasury.v1.GetFxHedgeRatesResponse;
import com.acme.treasury.v1.GetFxHedgeRulesRequest;
import com.acme.treasury.v1.GetFxHedgeRulesResponse;
import com.acme.treasury.v1.LintFxHedgesRequest;
import com.acme.treasury.v1.LintFxHedgesResponse;
import com.acme.treasury.v1.NormalizeFxHedgeRequest;
import com.acme.treasury.v1.NormalizeFxHedgeResponse;
import com.acme.treasury.v1.PreviewBankTransferRequest;
import com.acme.treasury.v1.PreviewBankTransferResponse;
import com.acme.treasury.v1.PreviewFxHedgeRequest;
import com.acme.treasury.v1.PreviewFxHedgeResponse;
import com.acme.treasury.v1.ScoreFxHedgeRequest;
import com.acme.treasury.v1.ScoreFxHedgeResponse;
import com.acme.treasury.v1.SimulateBankTransferRequest;
import com.acme.treasury.v1.SimulateBankTransferResponse;
import com.acme.treasury.v1.SimulateFxHedgeRequest;
import com.acme.treasury.v1.SimulateFxHedgeResponse;
import com.acme.treasury.v1.ValidateFxHedgeRequest;
import com.acme.treasury.v1.ValidateFxHedgeResponse;
import com.acme.venues.v1.CloneVenueRequest;
import com.acme.venues.v1.CloneVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueServiceGrpc;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
 * Checks fx hedges against the rules before anything acts on them. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class FxHedgeValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(FxHedgeValidationApplication.class);

    private final AudienceQueryServiceGrpc.AudienceQueryServiceBlockingStub audiencesAudienceQueryServiceBlocking;

    private final AuditEventServiceGrpc.AuditEventServiceBlockingStub auditAuditEventServiceBlocking;

    private final AuditTrailQueryServiceGrpc.AuditTrailQueryServiceBlockingStub auditAuditTrailQueryServiceBlocking;

    private final MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking;

    private final StatementQueryServiceGrpc.StatementQueryServiceBlockingStub bankingStatementQueryServiceBlocking;

    private final VarianceValidationServiceGrpc.VarianceValidationServiceBlockingStub budgetingVarianceValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final PolicyValidationServiceGrpc.PolicyValidationServiceBlockingStub insurancePolicyValidationServiceBlocking;

    private final TreasuryRepository repository;

    private final Executor rpcExecutor;

    private final SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking;

    private final BankTransferCalculationServiceGrpc.BankTransferCalculationServiceBlockingStub treasuryBankTransferCalculationServiceBlocking;

    private final BankTransferValidationServiceGrpc.BankTransferValidationServiceBlockingStub treasuryBankTransferValidationServiceBlocking;

    private final FxHedgeCalculationServiceGrpc.FxHedgeCalculationServiceBlockingStub treasuryFxHedgeCalculationServiceBlocking;

    private final VenueServiceGrpc.VenueServiceBlockingStub venuesVenueServiceBlocking;

    public FxHedgeValidationApplication(
            AudienceQueryServiceGrpc.AudienceQueryServiceBlockingStub audiencesAudienceQueryServiceBlocking,
            AuditEventServiceGrpc.AuditEventServiceBlockingStub auditAuditEventServiceBlocking,
            AuditTrailQueryServiceGrpc.AuditTrailQueryServiceBlockingStub auditAuditTrailQueryServiceBlocking,
            MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking,
            StatementQueryServiceGrpc.StatementQueryServiceBlockingStub bankingStatementQueryServiceBlocking,
            VarianceValidationServiceGrpc.VarianceValidationServiceBlockingStub budgetingVarianceValidationServiceBlocking,
            ChannelRegistry channels,
            PolicyValidationServiceGrpc.PolicyValidationServiceBlockingStub insurancePolicyValidationServiceBlocking,
            TreasuryRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking,
            BankTransferCalculationServiceGrpc.BankTransferCalculationServiceBlockingStub treasuryBankTransferCalculationServiceBlocking,
            BankTransferValidationServiceGrpc.BankTransferValidationServiceBlockingStub treasuryBankTransferValidationServiceBlocking,
            FxHedgeCalculationServiceGrpc.FxHedgeCalculationServiceBlockingStub treasuryFxHedgeCalculationServiceBlocking,
            VenueServiceGrpc.VenueServiceBlockingStub venuesVenueServiceBlocking) {
        this.audiencesAudienceQueryServiceBlocking = audiencesAudienceQueryServiceBlocking;
        this.auditAuditEventServiceBlocking = auditAuditEventServiceBlocking;
        this.auditAuditTrailQueryServiceBlocking = auditAuditTrailQueryServiceBlocking;
        this.bankingMandateCalculationServiceBlocking = bankingMandateCalculationServiceBlocking;
        this.bankingStatementQueryServiceBlocking = bankingStatementQueryServiceBlocking;
        this.budgetingVarianceValidationServiceBlocking = budgetingVarianceValidationServiceBlocking;
        this.channels = channels;
        this.insurancePolicyValidationServiceBlocking = insurancePolicyValidationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.searchSearchIndexQueryServiceBlocking = searchSearchIndexQueryServiceBlocking;
        this.treasuryBankTransferCalculationServiceBlocking = treasuryBankTransferCalculationServiceBlocking;
        this.treasuryBankTransferValidationServiceBlocking = treasuryBankTransferValidationServiceBlocking;
        this.treasuryFxHedgeCalculationServiceBlocking = treasuryFxHedgeCalculationServiceBlocking;
        this.venuesVenueServiceBlocking = venuesVenueServiceBlocking;
    }

    /**
     * Validates a fx hedge.
     */
    public ValidateFxHedgeResponse validateFxHedge(ValidateFxHedgeRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateFxHedge")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFxHedge(request.getContext().getTenantId(), request.getFxHedge());

        var outbound0 = ExportStatementsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportStatementsRequest.Filter.newBuilder()
                        .setRegion(request.getFxHedge().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream0 = bankingStatementQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportStatements(outbound0);
        var reply0 = ExportStatementsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getFxHedge().getFxHedgeId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateFxHedge", request.getFxHedge().getFxHedgeId());
        return ValidateFxHedgeResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setCounterparty(request.getFxHedge().getCounterparty())
                        .setFxHedgeId(request.getFxHedgeId())
                        .build())
                .build();
    }

    /**
     * Previews a fx hedge.
     */
    public PreviewFxHedgeResponse previewFxHedge(PreviewFxHedgeRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewFxHedge")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFxHedge(request.getContext().getTenantId(), request.getFxHedge());

        var outbound0 = ResolveSearchIndexesRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> searchSearchIndexQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveSearchIndexes(outbound0), rpcExecutor);

        var outbound1 = ValidatePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFxHedge().getAudit().getCreatedAt())
                                .setEtag(request.getFxHedge().getAudit().getEtag())
                                .build())
                        .setBank(PolicyBankFunding.newBuilder()
                                .setIban(request.getFxHedge().getBank().getIban())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> insurancePolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validatePolicy(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getFxHedge().getFxHedgeId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewFxHedge", request.getFxHedge().getFxHedgeId());
        return PreviewFxHedgeResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setBillingAddress(request.getFxHedge().getBillingAddress())
                        .setFxHedgeId(request.getFxHedgeId())
                        .build())
                .setOutcome(PreviewFxHedgeResponse.Outcome.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Checks a fx hedge.
     */
    public CheckFxHedgeEligibilityResponse checkFxHedgeEligibility(CheckFxHedgeEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckFxHedgeEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFxHedge(request.getContext().getTenantId(), request.getFxHedge());

        var results0 = new ArrayList<CompareBankTransferOptionsResponse>();
        for (var element : request.getFxHedge().getLinesList()) {
            CompareBankTransferOptionsRequest itemRequest = CompareBankTransferOptionsRequest.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .addLines(BankTransferLine.newBuilder()
                                .setTax(BankTransferLineTax.newBuilder()
                                        .setAmount(element.getTax().getAmount())
                                        .setRate(element.getTax().getRate())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
            results0.add(treasuryBankTransferCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareBankTransferOptions(itemRequest));
        }
        var reply0 = results0.isEmpty() ? CompareBankTransferOptionsResponse.getDefaultInstance()
                : results0.getLast();

        var futures1 = request.getFxHedge().getAttachmentsList().stream()
                .map(element -> CalculateMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .addAttachments(MandateAttachment.newBuilder()
                                .setChecksum(element.getChecksum())
                                .setFileName(element.getFileName())
                                .setSizeBytes(element.getSizeBytes())
                                .setUri(element.getUri())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> bankingMandateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateMandate(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? CalculateMandateResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getFxHedge().getFxHedgeId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckFxHedgeEligibility", request.getFxHedge().getFxHedgeId());
        return CheckFxHedgeEligibilityResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setCounterparty(request.getFxHedge().getCounterparty())
                        .setFxHedgeId(request.getFxHedgeId())
                        .build())
                .build();
    }

    /**
     * Normalizes a fx hedge.
     */
    public NormalizeFxHedgeResponse normalizeFxHedge(NormalizeFxHedgeRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeFxHedge")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFxHedge(request.getContext().getTenantId(), request.getFxHedge());

        var outbound0 = SimulateBankTransferRequest.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setCard(BankTransferCardSettlement.newBuilder()
                                .setLast4(request.getFxHedge().getCard().getLast4())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        RpcPort<SimulateBankTransferRequest, SimulateBankTransferResponse> port0 = RpcPort.adapt(treasuryBankTransferCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::simulateBankTransfer);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = ResolveAudiencesRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        RpcPort<ResolveAudiencesRequest, ResolveAudiencesResponse> port1 = RpcPort.adapt(audiencesAudienceQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::resolveAudiences);
        var reply1 = port1.exchange(outbound1);

        var results2 = new ArrayList<CalculateMandateResponse>();
        for (var element : request.getFxHedge().getAttachmentsList()) {
            CalculateMandateRequest itemRequest = CalculateMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .addAttachments(MandateAttachment.newBuilder()
                                .setChecksum(element.getChecksum())
                                .setContentType(element.getContentType())
                                .setUri(element.getUri())
                                .build())
                        .build())
                .build();
            results2.add(bankingMandateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateMandate(itemRequest));
        }
        var reply2 = results2.isEmpty() ? CalculateMandateResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getFxHedge().getFxHedgeId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeFxHedge", request.getFxHedge().getFxHedgeId());
        return NormalizeFxHedgeResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setBillingAddress(request.getFxHedge().getBillingAddress())
                        .setFxHedgeId(request.getFxHedgeId())
                        .setTotal(reply0.getBreakdown().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Explains a fx hedge.
     */
    public ExplainFxHedgeDecisionResponse explainFxHedgeDecision(ExplainFxHedgeDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainFxHedgeDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFxHedge(request.getContext().getTenantId(), request.getFxHedge());

        var outbound0 = PreviewBankTransferRequest.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getFxHedge().getAudit().getCreatedBy())
                                .build())
                        .setBank(BankTransferBankSettlement.newBuilder()
                                .setIban(switch (request.getFxHedge().getSettlementCase()) {
                                    case BANK -> request.getFxHedge().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PreviewBankTransferRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getContact().getName())
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .build();
        var reply0 = treasuryBankTransferValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewBankTransfer(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getFxHedge().getFxHedgeId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainFxHedgeDecision", request.getFxHedge().getFxHedgeId());
        return ExplainFxHedgeDecisionResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setDueAt(request.getFxHedge().getDueAt())
                        .setFxHedgeId(request.getFxHedgeId())
                        .build())
                .setOutcome(ExplainFxHedgeDecisionResponse.Outcome.newBuilder()
                        .setReferenceId(reply0.getOutcome().getReferenceId())
                        .build())
                .build();
    }

    /**
     * Scores a fx hedge.
     */
    public ScoreFxHedgeResponse scoreFxHedge(ScoreFxHedgeRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreFxHedge")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFxHedge(request.getContext().getTenantId(), request.getFxHedge());

        var outbound0 = PreviewBankTransferRequest.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getDetails().getDestination().getLinesList())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> treasuryBankTransferValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewBankTransfer(outbound0), rpcExecutor);

        var outbound1 = CloneVenueRequest.newBuilder()
                .setContext(request.getContext())
                .setVenue(Venue.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFxHedge().getAudit().getCreatedAt())
                                .setUpdatedBy(request.getFxHedge().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> venuesVenueServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneVenue(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = SimulateFxHedgeRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SimulateFxHedgeRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(switch (reply1.getVenue().getLocatorCase()) {
                                    case ADDRESS -> reply1.getVenue().getAddress().getRegion();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply2 = treasuryFxHedgeCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateFxHedge(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getFxHedge().getFxHedgeId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreFxHedge", request.getFxHedge().getFxHedgeId());
        return ScoreFxHedgeResponse.newBuilder()
                .setBreakdown(ScoreFxHedgeResponse.Breakdown.newBuilder()
                        .setCurrencyCode(reply2.getBreakdown().getCurrencyCode())
                        .build())
                .setFxHedge(FxHedge.newBuilder()
                        .setBillingAddress(reply0.getBankTransfer().getBillingAddress())
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setDisplayName(reply0.getBankTransfer().getCounterparty().getDisplayName())
                                .build())
                        .setDueAt(request.getFxHedge().getDueAt())
                        .setFxHedgeId(request.getFxHedgeId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Lints a fx hedge.
     */
    public LintFxHedgesResponse lintFxHedges(LintFxHedgesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintFxHedges")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFxHedge(request.getContext().getTenantId(), request.getFxHedge());

        var outbound0 = CompareCashPositionOptionsRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setCard(CashPositionCardFunding.newBuilder()
                                .setNetwork(request.getFxHedge().getCard().getNetwork())
                                .setToken(request.getFxHedge().getCard().getToken())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CompareCashPositionOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(request.getFxHedge().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.treasury.v1.CashPositionCalculationService");
        var selected0 = CashPositionCalculationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.compareCashPositionOptions(outbound0);

        var outbound1 = (ExplainVarianceDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setVariance(Variance.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply1 = budgetingVarianceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainVarianceDecision(outbound1);

        var outbound2 = GetFxHedgeRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setFxHedgeId(request.getFxHedge().getFxHedgeId())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(treasuryFxHedgeCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getFxHedgeRates(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound3 = UpdateAuditEventRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(AuditTrailRef.newBuilder()
                                .setDisplayName(reply0.getCashPosition().getHolder().getDisplayName())
                                .setNumber(switch (reply1.getVariance().getOriginCase()) {
                                    case CUSTOMER -> reply1.getVariance().getCustomer().getNumber();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply3 = auditAuditEventServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateAuditEvent(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getFxHedge().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintFxHedges", request.getFxHedge().getNumber());
        return LintFxHedgesResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setCurrencyCode(reply0.getBreakdown().getCurrencyCode())
                                .setDisplayName(reply0.getCashPosition().getHolder().getDisplayName())
                                .build())
                        .setDueAt(request.getFxHedge().getDueAt())
                        .setFxHedgeId(request.getFxHedgeId())
                        .setNumber(reply2.getFxHedge().getNumber())
                        .build())
                .setOutcome(LintFxHedgesResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply1.getOutcome().getNextStepsList())
                        .setReferenceId(reply1.getOutcome().getReferenceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply3.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a fx hedge.
     */
    public GetFxHedgeRulesResponse getFxHedgeRules(GetFxHedgeRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetFxHedgeRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FxHedge", request.getFxHedgeId(), "GetFxHedgeRules");

        var outbound0 = SuggestAuditTrailsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestAuditTrailsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .setOwnerId(request.getContext().getActor().getUserId())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> auditAuditTrailQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestAuditTrails(value)).orElseThrow();
        return GetFxHedgeRulesResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setFxHedgeId(request.getFxHedgeId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
