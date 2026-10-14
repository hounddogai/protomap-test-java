package com.acme.finance.domains.collections;

import com.acme.audit.v1.RecordRetentionPolicyVersionRequest;
import com.acme.audit.v1.RecordRetentionPolicyVersionResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.collections.v1.BatchCreateCollectionCasesRequest;
import com.acme.collections.v1.BatchCreateCollectionCasesResponse;
import com.acme.collections.v1.CheckWriteOffEligibilityRequest;
import com.acme.collections.v1.CheckWriteOffEligibilityResponse;
import com.acme.collections.v1.CloneCollectionCaseRequest;
import com.acme.collections.v1.CloneCollectionCaseResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseRef;
import com.acme.collections.v1.CreateCollectionCaseRequest;
import com.acme.collections.v1.CreateCollectionCaseResponse;
import com.acme.collections.v1.DeleteCollectionCaseRequest;
import com.acme.collections.v1.DeleteCollectionCaseResponse;
import com.acme.collections.v1.GetCollectionCaseRequest;
import com.acme.collections.v1.GetCollectionCaseResponse;
import com.acme.collections.v1.PreviewWriteOffRequest;
import com.acme.collections.v1.PreviewWriteOffResponse;
import com.acme.collections.v1.ScoreWriteOffRequest;
import com.acme.collections.v1.ScoreWriteOffResponse;
import com.acme.collections.v1.UpdateCollectionCaseRequest;
import com.acme.collections.v1.UpdateCollectionCaseResponse;
import com.acme.collections.v1.WatchCollectionCasesRequest;
import com.acme.collections.v1.WatchCollectionCasesResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CheckCoverageEligibilityRequest;
import com.acme.insurance.v1.CheckCoverageEligibilityResponse;
import com.acme.insurance.v1.CompareCoverageOptionsRequest;
import com.acme.insurance.v1.CompareCoverageOptionsResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.CoverageDiscountAction;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.CoverageValidationServiceGrpc;
import com.acme.insurance.v1.ExplainCoverageCalculationRequest;
import com.acme.insurance.v1.ExplainCoverageCalculationResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumValidationServiceGrpc;
import com.acme.insurance.v1.PreviewPremiumRequest;
import com.acme.insurance.v1.PreviewPremiumResponse;
import com.acme.insurance.v1.PreviewUnderwritingRequest;
import com.acme.insurance.v1.PreviewUnderwritingResponse;
import com.acme.insurance.v1.QuotePolicyRequest;
import com.acme.insurance.v1.QuotePolicyResponse;
import com.acme.insurance.v1.ScoreUnderwritingRequest;
import com.acme.insurance.v1.ScoreUnderwritingResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingComment;
import com.acme.insurance.v1.UnderwritingValidationServiceGrpc;
import com.acme.ledger.v1.CloneLedgerAccountRequest;
import com.acme.ledger.v1.CloneLedgerAccountResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountCardFunding;
import com.acme.ledger.v1.LedgerAccountServiceGrpc;
import com.acme.ledger.v1.ListLedgerAccountsRequest;
import com.acme.ledger.v1.ListLedgerAccountsResponse;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.SendPostingBatchTestNoticeRequest;
import com.acme.ledger.v1.SendPostingBatchTestNoticeResponse;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Manages collection cases: creates, reads, changes, and lists them. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class CollectionCaseApplication {
    private static final Logger log = LoggerFactory.getLogger(CollectionCaseApplication.class);

    private final RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking;

    private final WriteOffValidationServiceGrpc.WriteOffValidationServiceBlockingStub collectionsWriteOffValidationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking;

    private final CoverageValidationServiceGrpc.CoverageValidationServiceBlockingStub insuranceCoverageValidationServiceBlocking;

    private final PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking;

    private final PremiumValidationServiceGrpc.PremiumValidationServiceBlockingStub insurancePremiumValidationServiceBlocking;

    private final UnderwritingValidationServiceGrpc.UnderwritingValidationServiceBlockingStub insuranceUnderwritingValidationServiceBlocking;

    private final LedgerAccountServiceGrpc.LedgerAccountServiceBlockingStub ledgerLedgerAccountServiceBlocking;

    private final PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking;

    private final CollectionsRepository repository;

    private final Executor rpcExecutor;

    public CollectionCaseApplication(
            RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking,
            WriteOffValidationServiceGrpc.WriteOffValidationServiceBlockingStub collectionsWriteOffValidationServiceBlocking,
            ApplicationEventPublisher events,
            CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking,
            CoverageValidationServiceGrpc.CoverageValidationServiceBlockingStub insuranceCoverageValidationServiceBlocking,
            PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking,
            PremiumValidationServiceGrpc.PremiumValidationServiceBlockingStub insurancePremiumValidationServiceBlocking,
            UnderwritingValidationServiceGrpc.UnderwritingValidationServiceBlockingStub insuranceUnderwritingValidationServiceBlocking,
            LedgerAccountServiceGrpc.LedgerAccountServiceBlockingStub ledgerLedgerAccountServiceBlocking,
            PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking,
            CollectionsRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.auditRetentionPolicyHistoryServiceBlocking = auditRetentionPolicyHistoryServiceBlocking;
        this.collectionsWriteOffValidationServiceBlocking = collectionsWriteOffValidationServiceBlocking;
        this.events = events;
        this.insuranceCoverageCalculationServiceBlocking = insuranceCoverageCalculationServiceBlocking;
        this.insuranceCoverageValidationServiceBlocking = insuranceCoverageValidationServiceBlocking;
        this.insurancePolicyCalculationServiceBlocking = insurancePolicyCalculationServiceBlocking;
        this.insurancePremiumValidationServiceBlocking = insurancePremiumValidationServiceBlocking;
        this.insuranceUnderwritingValidationServiceBlocking = insuranceUnderwritingValidationServiceBlocking;
        this.ledgerLedgerAccountServiceBlocking = ledgerLedgerAccountServiceBlocking;
        this.ledgerPostingBatchNotificationServiceBlocking = ledgerPostingBatchNotificationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    public record GetCollectionCaseStage1Requested(PreviewWriteOffRequest request, CompletableFuture<PreviewWriteOffResponse> completion, io.grpc.Context context) {}

    /**
     * Creates a collection case.
     */
    public CreateCollectionCaseResponse createCollectionCase(CreateCollectionCaseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CreateCollectionCase")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollectionCase(request.getContext().getTenantId(), request.getCollectionCase());

        var futures0 = request.getCollectionCase().getCommentsList().stream()
                .map(element -> ExplainCoverageCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getCollectionCase().getAssignee().getEmail())
                                .build())
                        .build())
                .setDetails(ExplainCoverageCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(element.getAuthorEmail())
                                .setPhone(request.getCollectionCase().getRequester().getPhone())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> insuranceCoverageCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainCoverageCalculation(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? ExplainCoverageCalculationResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = ListLedgerAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListLedgerAccountsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getCollectionCase().getCustomer().getStartsAt())
                        .setOwnerId(request.getCollectionCase().getCollectionCaseId())
                        .build())
                .build();
        var reply1 = RpcSupport.retryRead(() -> ledgerLedgerAccountServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listLedgerAccounts(outbound1));
        log.atDebug().addKeyValue("auditValue", request.getCollectionCase().getSubject()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CreateCollectionCase", request.getCollectionCase().getSubject());
        return CreateCollectionCaseResponse.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setCollectionCaseId(request.getCollectionCase().getCollectionCaseId())
                        .setPriority(request.getCollectionCase().getPriority())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a collection case.
     */
    public GetCollectionCaseResponse getCollectionCase(GetCollectionCaseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetCollectionCase")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CollectionCase", request.getCollectionCaseId(), "GetCollectionCase");

        var outbound0 = RecordRetentionPolicyVersionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecordRetentionPolicyVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var reply0 = auditRetentionPolicyHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recordRetentionPolicyVersion(outbound0);

        var outbound1 = PreviewWriteOffRequest.newBuilder()
                .setContext(request.getContext())
                .setWriteOff(WriteOff.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getRetentionPolicy().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<PreviewWriteOffResponse>();
        events.publishEvent(new GetCollectionCaseStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();
        return GetCollectionCaseResponse.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply1.getWriteOff().getCounterparty().getDisplayName())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getRetentionPolicy().getAudit().getEtag())
                                .build())
                        .setCollectionCaseId(request.getCollectionCaseId())
                        .build())
                .build();
    }

    /**
     * Changes a collection case.
     */
    public UpdateCollectionCaseResponse updateCollectionCase(UpdateCollectionCaseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "UpdateCollectionCase")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollectionCase(request.getContext().getTenantId(), request.getCollectionCase());

        var outbound0 = PreviewWriteOffRequest.newBuilder()
                .setContext(request.getContext())
                .setWriteOff(WriteOff.newBuilder()
                        .setCounterparty(CollectionCaseRef.newBuilder()
                                .setDisplayName(request.getCollectionCase().getAssignee().getDisplayName())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            PreviewWriteOffResponse fetch(PreviewWriteOffRequest input) {
                return collectionsWriteOffValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewWriteOff(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = RpcSupport.unpack(Any.pack(CheckCoverageEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getWriteOff().getAudit().getEtag())
                                .build())
                        .build())
                .build()), CheckCoverageEligibilityRequest.class);
        var reply1 = insuranceCoverageValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkCoverageEligibility(outbound1);

        var outbound2 = CompareCoverageOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getWriteOff().getAudit().getCreatedAt())
                                .build())
                        .setDiscount(CoverageDiscountAction.newBuilder()
                                .setAmount(reply1.getCoverage().getDiscount().getAmount())
                                .setPercent(reply1.getCoverage().getDiscount().getPercent())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getCollectionCase().getAssignee().getUserId())
                                .build())
                        .build())
                .build();
        RpcPort<CompareCoverageOptionsRequest, CompareCoverageOptionsResponse> port2 = RpcPort.adapt(insuranceCoverageCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::compareCoverageOptions);
        var reply2 = port2.exchange(outbound2);

        var builder3 = PreviewPremiumRequest.newBuilder();
        builder3.setContext(request.getContext());
        builder3.setPremium(Premium.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setEtag(reply1.getCoverage().getAudit().getEtag())
                        .build())
                .setCounterparty(SupplierRef.newBuilder()
                        .setDisplayName(request.getCollectionCase().getCustomer().getDisplayName())
                        .build())
                .build());
        var outbound3 = builder3.build();
        var reply3 = insurancePremiumValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewPremium(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getCollectionCase().getCollectionCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "UpdateCollectionCase", request.getCollectionCase().getCollectionCaseId());
        return UpdateCollectionCaseResponse.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getWriteOff().getCounterparty().getDisplayName())
                                .setUserId(reply2.getCoverage().getOwner().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply3.getPremium().getAudit().getUpdatedBy())
                                .build())
                        .putAllCustomFields(request.getCollectionCase().getCustomFieldsMap())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply1.getCoverage().getOwner().getEmail())
                                .build())
                        .setStatus(request.getCollectionCase().getStatus())
                        .build())
                .build();
    }

    /**
     * Deletes a collection case.
     */
    public DeleteCollectionCaseResponse deleteCollectionCase(DeleteCollectionCaseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DeleteCollectionCase")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CollectionCase", request.getCollectionCaseId(), "DeleteCollectionCase");

        var outbound0 = ScoreUnderwritingRequest.newBuilder()
                .setContext(request.getContext())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getEtag())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.insurance.v1.UnderwritingValidationService/ScoreUnderwriting", outbound0);
        var reply0 = insuranceUnderwritingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreUnderwriting(command0.payload());

        var outbound1 = ScoreWriteOffRequest.newBuilder()
                .setContext(request.getContext())
                .setWriteOff(WriteOff.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getUnderwriting().getAudit().getCreatedAt())
                                .setUpdatedAt(reply0.getUnderwriting().getAudit().getUpdatedAt())
                                .build())
                        .setCounterparty(CollectionCaseRef.newBuilder()
                                .setPriority(reply0.getUnderwriting().getCustomer().getPriority())
                                .build())
                        .build())
                .build();
        final class Stage1Adapter {
            ScoreWriteOffResponse fetch(ScoreWriteOffRequest input) {
                return collectionsWriteOffValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreWriteOff(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);

        var outbound2 = PreviewUnderwritingRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewUnderwritingRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(reply0.getUnderwriting().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getUnderwriting().getAudit().getCreatedAt())
                                .build())
                        .addComments(UnderwritingComment.newBuilder()
                                .setCreatedAt(reply1.getWriteOff().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var reply2 = RpcSupport.call(outbound2, value -> insuranceUnderwritingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewUnderwriting(value));

        var outbound3 = CheckWriteOffEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckWriteOffEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getUnderwriting().getAssignee().getEmail())
                                .build())
                        .build())
                .setWriteOff(WriteOff.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getWriteOff().getAudit().getCreatedBy())
                                .setUpdatedAt(reply2.getUnderwriting().getAudit().getUpdatedAt())
                                .build())
                        .setCounterparty(CollectionCaseRef.newBuilder()
                                .setDisplayName(reply1.getWriteOff().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        final class Stage3Adapter {
            CheckWriteOffEligibilityResponse fetch(CheckWriteOffEligibilityRequest input) {
                return collectionsWriteOffValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkWriteOffEligibility(input);
            }
        }
        var reply3 = new Stage3Adapter().fetch(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getCollectionCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DeleteCollectionCase", request.getCollectionCaseId());
        return DeleteCollectionCaseResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Streams the changes of collection cases as they happen.
     */
    public WatchCollectionCasesResponse watchCollectionCases(WatchCollectionCasesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "WatchCollectionCases")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CollectionCase", request.getContext().getTraceId(), "WatchCollectionCases");

        var outbound0 = ListLedgerAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListLedgerAccountsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFilter().getCreatedAfter())
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .setOwnerId(request.getFilter().getOwnerId())
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.ledger.v1.LedgerAccountService/ListLedgerAccounts", outbound0);
        var reply0 = ledgerLedgerAccountServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listLedgerAccounts(command0.payload());
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "WatchCollectionCases", request.getFilter().getQuery());
        return WatchCollectionCasesResponse.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(request.getSubscriber().getPhone())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Takes a stream of collection cases and answers once at the end.
     */
    public BatchCreateCollectionCasesResponse batchCreateCollectionCases(BatchCreateCollectionCasesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BatchCreateCollectionCases")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        if (request.hasRecord()) {
            repository.saveCollectionCase(request.getContext().getTenantId(), request.getRecord());
        }

        var outbound0 = CheckWriteOffEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckWriteOffEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getHeader().getSubmittedBy().getName())
                                .build())
                        .build())
                .setWriteOff(WriteOff.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getRecord().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            CheckWriteOffEligibilityResponse fetch(CheckWriteOffEligibilityRequest input) {
                return collectionsWriteOffValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkWriteOffEligibility(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getHeader().getSource()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BatchCreateCollectionCases", request.getHeader().getSource());
        return BatchCreateCollectionCasesResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Creates a collection case.
     */
    public CloneCollectionCaseResponse cloneCollectionCase(CloneCollectionCaseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CloneCollectionCase")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollectionCase(request.getContext().getTenantId(), request.getCollectionCase());

        var outbound0 = SendPostingBatchTestNoticeRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SendPostingBatchTestNoticeRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setName(request.getCollectionCase().getRequester().getName())
                                .build())
                        .build())
                .build();
        var reply0 = ledgerPostingBatchNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).sendPostingBatchTestNotice(outbound0);

        var futures1 = request.getCollectionCase().getCommentsList().stream()
                .map(element -> QuotePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .setEtag(request.getCollectionCase().getAudit().getEtag())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(element.getAuthorEmail())
                                .build())
                        .setHolder(CoverageRef.newBuilder()
                                .setDisplayName(reply0.getPostingBatch().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePolicy(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? QuotePolicyResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = CloneLedgerAccountRequest.newBuilder()
                .setContext(request.getContext())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getPolicy().getAudit().getCreatedBy())
                                .build())
                        .setCard(LedgerAccountCardFunding.newBuilder()
                                .setToken(switch (reply1.getPolicy().getFundingCase()) {
                                    case CARD -> reply1.getPolicy().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getCollectionCase().getAssignee().getEmail())
                                .build())
                        .build())
                .build();
        var reply2 = ledgerLedgerAccountServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneLedgerAccount(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getCollectionCase().getCollectionCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CloneCollectionCase", request.getCollectionCase().getCollectionCaseId());
        return CloneCollectionCaseResponse.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply2.getLedgerAccount().getHolder().getDisplayName())
                                .build())
                        .setPriority(request.getCollectionCase().getPriority())
                        .setSubject(request.getCollectionCase().getSubject())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }
}
