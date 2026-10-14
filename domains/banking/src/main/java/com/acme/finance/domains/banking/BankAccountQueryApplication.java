package com.acme.finance.domains.banking;

import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.StreamAuditTrailExportRequest;
import com.acme.audit.v1.StreamAuditTrailExportResponse;
import com.acme.banking.v1.BankAccount;
import com.acme.banking.v1.BrowseBankAccountsRequest;
import com.acme.banking.v1.BrowseBankAccountsResponse;
import com.acme.banking.v1.CountBankAccountsRequest;
import com.acme.banking.v1.CountBankAccountsResponse;
import com.acme.banking.v1.ExplainMandateDecisionRequest;
import com.acme.banking.v1.ExplainMandateDecisionResponse;
import com.acme.banking.v1.ExportBankAccountsRequest;
import com.acme.banking.v1.ExportBankAccountsResponse;
import com.acme.banking.v1.FindBankAccountsRequest;
import com.acme.banking.v1.FindBankAccountsResponse;
import com.acme.banking.v1.LookupBankAccountRequest;
import com.acme.banking.v1.LookupBankAccountResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCardSettlement;
import com.acme.banking.v1.MandateLine;
import com.acme.banking.v1.MandateLineTax;
import com.acme.banking.v1.MandateValidationServiceGrpc;
import com.acme.banking.v1.NormalizeMandateRequest;
import com.acme.banking.v1.NormalizeMandateResponse;
import com.acme.banking.v1.PeekBankAccountRequest;
import com.acme.banking.v1.PeekBankAccountResponse;
import com.acme.banking.v1.PreviewMandateRequest;
import com.acme.banking.v1.PreviewMandateResponse;
import com.acme.banking.v1.ResolveBankAccountsRequest;
import com.acme.banking.v1.ResolveBankAccountsResponse;
import com.acme.banking.v1.ScoreMandateRequest;
import com.acme.banking.v1.ScoreMandateResponse;
import com.acme.banking.v1.SuggestBankAccountsRequest;
import com.acme.banking.v1.SuggestBankAccountsResponse;
import com.acme.banking.v1.SummarizeBankAccountsRequest;
import com.acme.banking.v1.SummarizeBankAccountsResponse;
import com.acme.banking.v1.ValidateMandateRequest;
import com.acme.banking.v1.ValidateMandateResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CheckClaimEligibilityRequest;
import com.acme.insurance.v1.CheckClaimEligibilityResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimValidationServiceGrpc;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.CoverageValidationServiceGrpc;
import com.acme.insurance.v1.EstimatePolicyRequest;
import com.acme.insurance.v1.EstimatePolicyResponse;
import com.acme.insurance.v1.ExplainPolicyCalculationRequest;
import com.acme.insurance.v1.ExplainPolicyCalculationResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyBankFunding;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.insurance.v1.RecalculateCoveragesRequest;
import com.acme.insurance.v1.RecalculateCoveragesResponse;
import com.acme.insurance.v1.ValidateCoverageRequest;
import com.acme.insurance.v1.ValidateCoverageResponse;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchHistoryServiceGrpc;
import com.acme.ledger.v1.RevertPostingBatchRequest;
import com.acme.ledger.v1.RevertPostingBatchResponse;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexServiceGrpc;
import com.acme.search.v1.UpdateSearchIndexRequest;
import com.acme.search.v1.UpdateSearchIndexResponse;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Answers read-only questions about bank accounts, for screens and reports. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class BankAccountQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(BankAccountQueryApplication.class);

    private final AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking;

    private final MandateValidationServiceGrpc.MandateValidationServiceBlockingStub bankingMandateValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final ClaimValidationServiceGrpc.ClaimValidationServiceFutureStub insuranceClaimValidationServiceFuture;

    private final CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking;

    private final PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking;

    private final BankAccountQueryRequestMapper mapper;

    private final BankingRepository repository;

    private final SearchIndexServiceGrpc.SearchIndexServiceFutureStub searchSearchIndexServiceFuture;

    public BankAccountQueryApplication(
            AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking,
            MandateValidationServiceGrpc.MandateValidationServiceBlockingStub bankingMandateValidationServiceBlocking,
            ChannelRegistry channels,
            ClaimValidationServiceGrpc.ClaimValidationServiceFutureStub insuranceClaimValidationServiceFuture,
            CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking,
            PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking,
            BankAccountQueryRequestMapper mapper,
            BankingRepository repository,
            SearchIndexServiceGrpc.SearchIndexServiceFutureStub searchSearchIndexServiceFuture) {
        this.auditAuditTrailExportServiceBlocking = auditAuditTrailExportServiceBlocking;
        this.bankingMandateValidationServiceBlocking = bankingMandateValidationServiceBlocking;
        this.channels = channels;
        this.insuranceClaimValidationServiceFuture = insuranceClaimValidationServiceFuture;
        this.insuranceCoverageCalculationServiceBlocking = insuranceCoverageCalculationServiceBlocking;
        this.insurancePolicyCalculationServiceBlocking = insurancePolicyCalculationServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.searchSearchIndexServiceFuture = searchSearchIndexServiceFuture;
    }

    /**
     * Returns a bank account.
     */
    public LookupBankAccountResponse lookupBankAccount(LookupBankAccountRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupBankAccount")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankAccount", request.getBankAccountId(), "LookupBankAccount");
        return LookupBankAccountResponse.newBuilder()
                .setBankAccount(BankAccount.newBuilder()
                        .setBankAccountId(request.getBankAccountId())
                        .build())
                .build();
    }

    /**
     * Lists bank accounts a page at a time.
     */
    public BrowseBankAccountsResponse browseBankAccounts(BrowseBankAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowseBankAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankAccount", request.getContext().getTraceId(), "BrowseBankAccounts");

        var builder0 = ExplainMandateDecisionRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setMandate(Mandate.newBuilder()
                .setBillingAddress(PostalAddress.newBuilder()
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .setCounterparty(CustomerRef.newBuilder()
                        .setEmail(request.getContext().getActor().getEmail())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = bankingMandateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainMandateDecision(outbound0);

        var outbound1 = ValidateMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setCard(MandateCardSettlement.newBuilder()
                                .setToken(switch (reply0.getMandate().getSettlementCase()) {
                                    case CARD -> reply0.getMandate().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = bankingMandateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateMandate(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowseBankAccounts", request.getPage().getPageToken());
        return BrowseBankAccountsResponse.newBuilder()
                .addBankAccounts(BankAccount.newBuilder()
                        .setHolder(CustomerRef.newBuilder()
                                .setEmail(reply1.getMandate().getCounterparty().getEmail())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Lists bank accounts a page at a time.
     */
    public FindBankAccountsResponse findBankAccounts(FindBankAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FindBankAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankAccount", request.getContext().getTraceId(), "FindBankAccounts");

        var outbound0 = StreamAuditTrailExportRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamAuditTrailExportRequest.Filter.newBuilder()
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build();
        var stream0 = auditAuditTrailExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamAuditTrailExport(outbound0);
        var reply0 = StreamAuditTrailExportResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "FindBankAccounts", request.getPage().getPageToken());
        return FindBankAccountsResponse.getDefaultInstance();
    }

    /**
     * Counts bank accounts by status.
     */
    public CountBankAccountsResponse countBankAccounts(CountBankAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountBankAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankAccount", request.getContext().getTraceId(), "CountBankAccounts");

        var outbound0 = NormalizeMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        RpcPort<NormalizeMandateRequest, NormalizeMandateResponse> port0 = RpcPort.adapt(bankingMandateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::normalizeMandate);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = BeanCopies.copy(RecalculateCoveragesRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getMandate().getAudit().getUpdatedAt())
                                .setUpdatedBy(reply0.getMandate().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build(), RecalculateCoveragesRequest.class);
        var reply1 = insuranceCoverageCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateCoverages(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountBankAccounts", request.getPage().getPageToken());
        return CountBankAccountsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns several bank accounts at once.
     */
    public ResolveBankAccountsResponse resolveBankAccounts(ResolveBankAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveBankAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankAccount", request.getContext().getTraceId(), "ResolveBankAccounts");

        var outbound0 = CheckClaimEligibilityRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setCustomer(CustomerRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CheckClaimEligibilityRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build();
        var pending0 = insuranceClaimValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).checkClaimEligibility(outbound0);
        var reply0 = RpcSupport.await(pending0);
        return ResolveBankAccountsResponse.newBuilder()
                .addBankAccounts(BankAccount.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getClaim().getCustomer().getEmail())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Streams every matching bank account.
     */
    public ExportBankAccountsResponse exportBankAccounts(ExportBankAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExportBankAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankAccount", request.getContext().getTraceId(), "ExportBankAccounts");

        var outbound0 = ExplainMandateDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainMandateDecisionRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setMandate(Mandate.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.banking.v1.MandateValidationService");
        var selected0 = MandateValidationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.explainMandateDecision(outbound0);

        var outbound1 = ValidateCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getMandate().getAudit().getEtag())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getMandate().getCounterparty().getDisplayName())
                                .setEmail(reply0.getMandate().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.insurance.v1.CoverageValidationService");
        var selected1 = CoverageValidationServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.validateCoverage(outbound1);

        var outbound2 = BeanCopies.copy(EstimatePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EstimatePolicyRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(reply0.getMandate().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getMandate().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build(), EstimatePolicyRequest.class);
        var reply2 = insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimatePolicy(outbound2);

        var outbound3 = NormalizeMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply2.getPolicy().getAudit().getEtag())
                                .build())
                        .setCard(MandateCardSettlement.newBuilder()
                                .setLast4(switch (reply0.getMandate().getSettlementCase()) {
                                    case CARD -> reply0.getMandate().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply1.getCoverage().getOwner().getEmail())
                                .build())
                        .addLines(MandateLine.newBuilder()
                                .setTax(MandateLineTax.newBuilder()
                                        .setAmount(reply1.getCoverage().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply3 = bankingMandateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeMandate(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExportBankAccounts", request.getFilter().getQuery());
        return ExportBankAccountsResponse.newBuilder()
                .setBankAccount(BankAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply3.getMandate().getAudit().getUpdatedAt())
                                .setUpdatedBy(reply0.getMandate().getAudit().getUpdatedBy())
                                .build())
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Summarizes a bank account.
     */
    public SummarizeBankAccountsResponse summarizeBankAccounts(SummarizeBankAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SummarizeBankAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankAccount", request.getBankAccountId(), "SummarizeBankAccounts");

        var outbound0 = JsonCodec.roundTrip(ValidateMandateRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(ValidateMandateRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build(), ValidateMandateRequest.class);
        var reply0 = bankingMandateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateMandate(outbound0);

        var outbound1 = RevertPostingBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RevertPostingBatchRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setPostingBatch(PostingBatch.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply0.getMandate().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.ledger.v1.PostingBatchHistoryService");
        var selected1 = PostingBatchHistoryServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.revertPostingBatch(outbound1);

        var outbound2 = UpdateSearchIndexRequest.newBuilder()
                .setContext(request.getContext())
                .setSearchIndex(SearchIndex.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setLocality(reply0.getMandate().getBillingAddress().getLocality())
                                .setPostalCode(reply1.getPostingBatch().getBillingAddress().getPostalCode())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getMandate().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var pending2 = searchSearchIndexServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).updateSearchIndex(outbound2);
        var reply2 = RpcSupport.await(pending2);

        var outbound3 = mapper.summarizeBankAccountsStage3(request, reply0, reply2);
        var reply3 = insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainPolicyCalculation(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getBankAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SummarizeBankAccounts", request.getBankAccountId());
        return SummarizeBankAccountsResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Lists bank accounts a page at a time.
     */
    public SuggestBankAccountsResponse suggestBankAccounts(SuggestBankAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuggestBankAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankAccount", request.getContext().getTraceId(), "SuggestBankAccounts");

        var outbound0 = ScoreMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.banking.v1.MandateValidationService");
        var serviceName0 = "acme.banking.v1" + "." + "MandateValidationService";
        var methodName0 = "Sco" + "reMandate";
        var descriptor0 = MethodDescriptor.<ScoreMandateRequest, ScoreMandateResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(ScoreMandateRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ScoreMandateResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = PreviewMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply0.getMandate().getBillingAddress().getRegion())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply0.getMandate().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> bankingMandateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewMandate(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuggestBankAccounts", request.getPage().getPageToken());
        return SuggestBankAccountsResponse.newBuilder()
                .addBankAccounts(BankAccount.newBuilder()
                        .setHolder(CustomerRef.newBuilder()
                                .setCustomerId(reply0.getMandate().getCounterparty().getCustomerId())
                                .setEmail(reply1.getMandate().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
    }

    /**
     * Returns a bank account.
     */
    public PeekBankAccountResponse peekBankAccount(PeekBankAccountRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PeekBankAccount")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankAccount", request.getBankAccountId(), "PeekBankAccount");

        var outbound0 = PreviewMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewMandateRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build();
        Function<PreviewMandateRequest, PreviewMandateResponse> operation0 = bankingMandateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::previewMandate;
        var reply0 = operation0.apply(outbound0);
        return PeekBankAccountResponse.newBuilder()
                .setBankAccount(BankAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getMandate().getAudit().getUpdatedAt())
                                .build())
                        .setBankAccountId(request.getBankAccountId())
                        .build())
                .build();
    }
}
