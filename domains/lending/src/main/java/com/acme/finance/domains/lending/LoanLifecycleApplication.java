package com.acme.finance.domains.lending;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailCardSettlement;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.AuditTrailSyncServiceGrpc;
import com.acme.audit.v1.ResolveAuditTrailConflictRequest;
import com.acme.audit.v1.ResolveAuditTrailConflictResponse;
import com.acme.audit.v1.RetentionPolicyRef;
import com.acme.audit.v1.RevertAuditTrailRequest;
import com.acme.audit.v1.RevertAuditTrailResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateApprovalServiceGrpc;
import com.acme.banking.v1.MandateLine;
import com.acme.banking.v1.MandateLineTax;
import com.acme.banking.v1.SubmitMandateRequest;
import com.acme.banking.v1.SubmitMandateResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.PaymentPromiseRef;
import com.acme.collections.v1.RetryCollectionCaseWorkflowRequest;
import com.acme.collections.v1.RetryCollectionCaseWorkflowResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.concessions.v1.BrowseConcessionStandsRequest;
import com.acme.concessions.v1.BrowseConcessionStandsResponse;
import com.acme.concessions.v1.ConcessionStandQueryServiceGrpc;
import com.acme.customers.v1.ComparePreferenceOptionsRequest;
import com.acme.customers.v1.ComparePreferenceOptionsResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.ExplainCustomerSegmentCalculationRequest;
import com.acme.customers.v1.ExplainCustomerSegmentCalculationResponse;
import com.acme.customers.v1.NormalizeConsentRecordRequest;
import com.acme.customers.v1.NormalizeConsentRecordResponse;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.RecalculatePreferencesRequest;
import com.acme.customers.v1.RecalculatePreferencesResponse;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentBankSettlement;
import com.acme.documents.v1.DocumentLine;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.documents.v1.PreviewDocumentRequest;
import com.acme.documents.v1.PreviewDocumentResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.fuel.v1.ExplainFuelCardCalculationRequest;
import com.acme.fuel.v1.ExplainFuelCardCalculationResponse;
import com.acme.fuel.v1.FuelCard;
import com.acme.fuel.v1.FuelCardCalculationServiceGrpc;
import com.acme.fuel.v1.FuelCardCardFunding;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PullPremiumsRequest;
import com.acme.insurance.v1.PullPremiumsResponse;
import com.acme.lending.v1.ActivateLoanRequest;
import com.acme.lending.v1.ActivateLoanResponse;
import com.acme.lending.v1.FindLoansRequest;
import com.acme.lending.v1.FindLoansResponse;
import com.acme.lending.v1.GetLoanLifecycleRequest;
import com.acme.lending.v1.GetLoanLifecycleResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplicationRef;
import com.acme.lending.v1.LoanLimit;
import com.acme.lending.v1.LoanQueryServiceGrpc;
import com.acme.lending.v1.ReinstateLoanRequest;
import com.acme.lending.v1.ReinstateLoanResponse;
import com.acme.lending.v1.RenewLoanRequest;
import com.acme.lending.v1.RenewLoanResponse;
import com.acme.lending.v1.ScheduleLoanExpiryRequest;
import com.acme.lending.v1.ScheduleLoanExpiryResponse;
import com.acme.lending.v1.SuspendLoanRequest;
import com.acme.lending.v1.SuspendLoanResponse;
import com.acme.lending.v1.TerminateLoanRequest;
import com.acme.lending.v1.TerminateLoanResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.acme.search.v1.SummarizeSearchIndexesRequest;
import com.acme.search.v1.SummarizeSearchIndexesResponse;
import com.acme.suppliers.v1.SupplierRef;
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
import org.springframework.stereotype.Service;

/**
 * Moves a loan between the states of its lifecycle. Coordinates validation, durable audit writes, and downstream
 * operations.
 */
@Service
public class LoanLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(LoanLifecycleApplication.class);

    private final AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking;

    private final AuditTrailSyncServiceGrpc.AuditTrailSyncServiceBlockingStub auditAuditTrailSyncServiceBlocking;

    private final MandateApprovalServiceGrpc.MandateApprovalServiceFutureStub bankingMandateApprovalServiceFuture;

    private final ChannelRegistry channels;

    private final ConcessionStandQueryServiceGrpc.ConcessionStandQueryServiceBlockingStub concessionsConcessionStandQueryServiceBlocking;

    private final ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceBlockingStub customersConsentRecordValidationServiceBlocking;

    private final CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking;

    private final PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking;

    private final DocumentValidationServiceGrpc.DocumentValidationServiceBlockingStub documentsDocumentValidationServiceBlocking;

    private final FuelCardCalculationServiceGrpc.FuelCardCalculationServiceBlockingStub fuelFuelCardCalculationServiceBlocking;

    private final LoanQueryServiceGrpc.LoanQueryServiceBlockingStub lendingLoanQueryServiceBlocking;

    private final LendingRepository repository;

    private final Executor rpcExecutor;

    private final SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking;

    private final SearchIndexQueryServiceGrpc.SearchIndexQueryServiceFutureStub searchSearchIndexQueryServiceFuture;

    public LoanLifecycleApplication(
            AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking,
            AuditTrailSyncServiceGrpc.AuditTrailSyncServiceBlockingStub auditAuditTrailSyncServiceBlocking,
            MandateApprovalServiceGrpc.MandateApprovalServiceFutureStub bankingMandateApprovalServiceFuture,
            ChannelRegistry channels,
            ConcessionStandQueryServiceGrpc.ConcessionStandQueryServiceBlockingStub concessionsConcessionStandQueryServiceBlocking,
            ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceBlockingStub customersConsentRecordValidationServiceBlocking,
            CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking,
            PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking,
            DocumentValidationServiceGrpc.DocumentValidationServiceBlockingStub documentsDocumentValidationServiceBlocking,
            FuelCardCalculationServiceGrpc.FuelCardCalculationServiceBlockingStub fuelFuelCardCalculationServiceBlocking,
            LoanQueryServiceGrpc.LoanQueryServiceBlockingStub lendingLoanQueryServiceBlocking,
            LendingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking,
            SearchIndexQueryServiceGrpc.SearchIndexQueryServiceFutureStub searchSearchIndexQueryServiceFuture) {
        this.auditAuditTrailHistoryServiceBlocking = auditAuditTrailHistoryServiceBlocking;
        this.auditAuditTrailSyncServiceBlocking = auditAuditTrailSyncServiceBlocking;
        this.bankingMandateApprovalServiceFuture = bankingMandateApprovalServiceFuture;
        this.channels = channels;
        this.concessionsConcessionStandQueryServiceBlocking = concessionsConcessionStandQueryServiceBlocking;
        this.customersConsentRecordValidationServiceBlocking = customersConsentRecordValidationServiceBlocking;
        this.customersCustomerSegmentCalculationServiceBlocking = customersCustomerSegmentCalculationServiceBlocking;
        this.customersPreferenceCalculationServiceBlocking = customersPreferenceCalculationServiceBlocking;
        this.documentsDocumentValidationServiceBlocking = documentsDocumentValidationServiceBlocking;
        this.fuelFuelCardCalculationServiceBlocking = fuelFuelCardCalculationServiceBlocking;
        this.lendingLoanQueryServiceBlocking = lendingLoanQueryServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.searchSearchIndexQueryServiceBlocking = searchSearchIndexQueryServiceBlocking;
        this.searchSearchIndexQueryServiceFuture = searchSearchIndexQueryServiceFuture;
    }

    /**
     * Activates a loan.
     */
    public ActivateLoanResponse activateLoan(ActivateLoanRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivateLoan")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Loan", request.getLoanId(), "ActivateLoan");

        var outbound0 = FindLoansRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindLoansRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> lendingLoanQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findLoans(outbound0), rpcExecutor);

        var outbound1 = ExplainCustomerSegmentCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .build())
                .setDetails(ExplainCustomerSegmentCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> customersCustomerSegmentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainCustomerSegmentCalculation(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivateLoan", request.getDetails().getReasonCode());
        return ActivateLoanResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .setHolder(LoanApplicationRef.newBuilder()
                                .setDisplayName(reply1.getCustomerSegment().getOwner().getDisplayName())
                                .build())
                        .setLoanId(request.getLoanId())
                        .build())
                .setOutcome(ActivateLoanResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply1.getOutcome().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Suspends a loan.
     */
    public SuspendLoanResponse suspendLoan(SuspendLoanRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendLoan")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Loan", request.getLoanId(), "SuspendLoan");

        var outbound0 = SummarizeSearchIndexesRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build();
        RpcPort<SummarizeSearchIndexesRequest, SummarizeSearchIndexesResponse> port0 = RpcPort.adapt(searchSearchIndexQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::summarizeSearchIndexes);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = RevertAuditTrailRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setAuditTrail(AuditTrail.newBuilder()
                        .setCounterparty(RetentionPolicyRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RevertAuditTrailRequest.Details.newBuilder()
                        .setAuthorEmail(request.getDetails().getNewOwner().getEmail())
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(auditAuditTrailHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).revertAuditTrail(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendLoan", request.getDetails().getReasonCode());
        return SuspendLoanResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .setLoanId(request.getLoanId())
                        .build())
                .setOutcome(SuspendLoanResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Reinstates a loan.
     */
    public ReinstateLoanResponse reinstateLoan(ReinstateLoanRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReinstateLoan")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Loan", request.getLoanId(), "ReinstateLoan");

        var outbound0 = JsonCodec.roundTrip(ComparePreferenceOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setPreference(Preference.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .build(), ComparePreferenceOptionsRequest.class);
        var reply0 = customersPreferenceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).comparePreferenceOptions(outbound0);

        var outbound1 = SubmitMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubmitMandateRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(reply0.getPreference().getOwner().getDisplayName())
                                .setEmail(reply0.getPreference().getOwner().getEmail())
                                .build())
                        .build())
                .setMandate(Mandate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getPreference().getAudit().getUpdatedBy())
                                .build())
                        .addLines(MandateLine.newBuilder()
                                .setTax(MandateLineTax.newBuilder()
                                        .setAmount(reply0.getPreference().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var pending1 = bankingMandateApprovalServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).submitMandate(outbound1);
        var reply1 = RpcSupport.await(pending1);

        var outbound2 = ResolveAuditTrailConflictRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply1.getMandate().getBillingAddress().getRecipient())
                                .build())
                        .setCard(AuditTrailCardSettlement.newBuilder()
                                .setLast4(switch (reply1.getMandate().getSettlementCase()) {
                                    case CARD -> reply1.getMandate().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ResolveAuditTrailConflictRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var reply2 = auditAuditTrailSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveAuditTrailConflict(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReinstateLoan", request.getDetails().getReasonCode());
        return ReinstateLoanResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .addLimits(LoanLimit.newBuilder()
                                .setAmount(reply0.getPreference().getDiscount().getAmount())
                                .build())
                        .setLoanId(request.getLoanId())
                        .build())
                .setOutcome(ReinstateLoanResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .addAllNextSteps(reply1.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Terminates a loan.
     */
    public TerminateLoanResponse terminateLoan(TerminateLoanRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TerminateLoan")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Loan", request.getLoanId(), "TerminateLoan");

        var outbound0 = NormalizeConsentRecordRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .build())
                .setContext(request.getContext())
                .setDetails(NormalizeConsentRecordRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.customers.v1.ConsentRecordValidationService/NormalizeConsentRecord", outbound0);
        var reply0 = customersConsentRecordValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeConsentRecord(command0.payload());

        var outbound1 = RetryCollectionCaseWorkflowRequest.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setCustomer(PaymentPromiseRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RetryCollectionCaseWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.collections.v1.CollectionCaseWorkflowService");
        var serviceName1 = "acme.collections.v1" + "." + "CollectionCaseWorkflowService";
        var methodName1 = "Ret" + "ryCollectionCaseWorkflow";
        var descriptor1 = MethodDescriptor.<RetryCollectionCaseWorkflowRequest, RetryCollectionCaseWorkflowResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(RetryCollectionCaseWorkflowRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(RetryCollectionCaseWorkflowResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TerminateLoan", request.getDetails().getReasonCode());
        return TerminateLoanResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .setLoanId(request.getLoanId())
                        .build())
                .setOutcome(TerminateLoanResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Renews a loan.
     */
    public RenewLoanResponse renewLoan(RenewLoanRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RenewLoan")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Loan", request.getLoanId(), "RenewLoan");

        var outbound0 = PullPremiumsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PullPremiumsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setPremium(Premium.newBuilder()
                        .setCounterparty(SupplierRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build();
        var channel0 = channels.lookup("acme.insurance.v1.PremiumSyncService");
        var serviceName0 = "acme.insurance.v1" + "." + "PremiumSyncService";
        var methodName0 = "Pul" + "lPremiums";
        var descriptor0 = MethodDescriptor.<PullPremiumsRequest, PullPremiumsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(PullPremiumsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(PullPremiumsResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = SummarizeSearchIndexesRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SummarizeSearchIndexesRequest.Details.newBuilder()
                        .putAllFilters(reply0.getPremium().getMetadataMap())
                        .build())
                .setReason(request.getDetails().getReasonCode())
                .build();
        var pending1 = searchSearchIndexQueryServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeSearchIndexes(outbound1);
        var reply1 = RpcSupport.await(pending1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getNewOwner().getUserId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RenewLoan", request.getDetails().getNewOwner().getUserId());
        return RenewLoanResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .setLoanId(request.getLoanId())
                        .build())
                .setOutcome(RenewLoanResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a loan.
     */
    public GetLoanLifecycleResponse getLoanLifecycle(GetLoanLifecycleRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetLoanLifecycle")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Loan", request.getLoanId(), "GetLoanLifecycle");

        var outbound0 = BrowseConcessionStandsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseConcessionStandsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .putAllLabels(request.getContext().getBaggageMap())
                        .setOwnerId(request.getLoanId())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> concessionsConcessionStandQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseConcessionStands(value)).orElseThrow();
        return GetLoanLifecycleResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .setLoanId(request.getLoanId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Schedules a loan.
     */
    public ScheduleLoanExpiryResponse scheduleLoanExpiry(ScheduleLoanExpiryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleLoanExpiry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoan(request.getContext().getTenantId(), request.getLoan());

        var outbound0 = NormalizeConsentRecordRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .build())
                .setContext(request.getContext())
                .setDetails(NormalizeConsentRecordRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getLoan().getContact().getName())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> customersConsentRecordValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeConsentRecord(outbound0), rpcExecutor);

        var outbound1 = ExplainFuelCardCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setFuelCard(FuelCard.newBuilder()
                        .setCard(FuelCardCardFunding.newBuilder()
                                .setHolderName(request.getLoan().getCard().getHolderName())
                                .setLast4(request.getLoan().getCard().getLast4())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> fuelFuelCardCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainFuelCardCalculation(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = RecalculatePreferencesRequest.newBuilder()
                .setContext(request.getContext())
                .setPreference(Preference.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply1.getFuelCard().getHolder().getDisplayName())
                                .build())
                        .build())
                .build();
        var adapter2 = new Function<RecalculatePreferencesRequest, RecalculatePreferencesResponse>() {
            @Override
            public RecalculatePreferencesResponse apply(RecalculatePreferencesRequest input) {
                return customersPreferenceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculatePreferences(input);
            }
        };
        var reply2 = adapter2.apply(outbound2);

        var futures3 = request.getLoan().getLimitsList().stream()
                .map(element -> PreviewDocumentRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewDocumentRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(reply1.getFuelCard().getContact().getPhone())
                                .build())
                        .build())
                .setDocument(Document.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getLoan().getAudit().getUpdatedBy())
                                .build())
                        .setBank(DocumentBankSettlement.newBuilder()
                                .setIban(request.getLoan().getBank().getIban())
                                .build())
                        .addLines(DocumentLine.newBuilder()
                                .setAmount(element.getAmount())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> documentsDocumentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewDocument(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures3.toArray(CompletableFuture[]::new)).join();
        var results3 = futures3.stream().map(CompletableFuture::join).toList();
        var reply3 = results3.isEmpty() ? PreviewDocumentResponse.getDefaultInstance()
                : results3.getLast();
        log.atDebug().addKeyValue("auditValue", request.getLoan().getLoanId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleLoanExpiry", request.getLoan().getLoanId());
        return ScheduleLoanExpiryResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getFuelCard().getAudit().getUpdatedAt())
                                .build())
                        .setAvailable(reply1.getFuelCard().getAvailable())
                        .setContact(request.getLoan().getContact())
                        .setLoanId(request.getLoanId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .setMessage(reply0.getOutcome().getMessage())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
