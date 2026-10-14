package com.acme.finance.domains.collections;

import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.collections.v1.AdvancePaymentPromiseWorkflowRequest;
import com.acme.collections.v1.AdvancePaymentPromiseWorkflowResponse;
import com.acme.collections.v1.BrowsePaymentPromisesRequest;
import com.acme.collections.v1.BrowsePaymentPromisesResponse;
import com.acme.collections.v1.CancelPaymentPromiseWorkflowRequest;
import com.acme.collections.v1.CancelPaymentPromiseWorkflowResponse;
import com.acme.collections.v1.CloneCollectionCaseRequest;
import com.acme.collections.v1.CloneCollectionCaseResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseApprovalServiceGrpc;
import com.acme.collections.v1.CollectionCaseComment;
import com.acme.collections.v1.CollectionCaseQueryServiceGrpc;
import com.acme.collections.v1.CollectionCaseRef;
import com.acme.collections.v1.CollectionCaseServiceGrpc;
import com.acme.collections.v1.ExportCollectionCasesRequest;
import com.acme.collections.v1.ExportCollectionCasesResponse;
import com.acme.collections.v1.InspectPaymentPromiseWorkflowRequest;
import com.acme.collections.v1.InspectPaymentPromiseWorkflowResponse;
import com.acme.collections.v1.ListPaymentPromiseWorkflowsRequest;
import com.acme.collections.v1.ListPaymentPromiseWorkflowsResponse;
import com.acme.collections.v1.NormalizeWriteOffRequest;
import com.acme.collections.v1.NormalizeWriteOffResponse;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PaymentPromiseParticipant;
import com.acme.collections.v1.PaymentPromiseQueryServiceGrpc;
import com.acme.collections.v1.RecallWriteOffRequest;
import com.acme.collections.v1.RecallWriteOffResponse;
import com.acme.collections.v1.RejectWriteOffRequest;
import com.acme.collections.v1.RejectWriteOffResponse;
import com.acme.collections.v1.ResolveCollectionCasesRequest;
import com.acme.collections.v1.ResolveCollectionCasesResponse;
import com.acme.collections.v1.ResumePaymentPromiseWorkflowRequest;
import com.acme.collections.v1.ResumePaymentPromiseWorkflowResponse;
import com.acme.collections.v1.RetryPaymentPromiseWorkflowRequest;
import com.acme.collections.v1.RetryPaymentPromiseWorkflowResponse;
import com.acme.collections.v1.SignalPaymentPromiseWorkflowRequest;
import com.acme.collections.v1.SignalPaymentPromiseWorkflowResponse;
import com.acme.collections.v1.StartPaymentPromiseWorkflowRequest;
import com.acme.collections.v1.StartPaymentPromiseWorkflowResponse;
import com.acme.collections.v1.SubmitCollectionCaseRequest;
import com.acme.collections.v1.SubmitCollectionCaseResponse;
import com.acme.collections.v1.SubmitWriteOffRequest;
import com.acme.collections.v1.SubmitWriteOffResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffApprovalServiceGrpc;
import com.acme.collections.v1.WriteOffBankSettlement;
import com.acme.collections.v1.WriteOffCardSettlement;
import com.acme.collections.v1.WriteOffValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.forms.v1.CompareFormOptionsRequest;
import com.acme.forms.v1.CompareFormOptionsResponse;
import com.acme.forms.v1.Form;
import com.acme.forms.v1.FormBankSettlement;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CountCoverageTotalsRequest;
import com.acme.insurance.v1.CountCoverageTotalsResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageReportingServiceGrpc;
import com.acme.insurance.v1.CoverageWorkflowServiceGrpc;
import com.acme.insurance.v1.DelegatePremiumApprovalRequest;
import com.acme.insurance.v1.DelegatePremiumApprovalResponse;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumApprovalServiceGrpc;
import com.acme.insurance.v1.RetryCoverageWorkflowRequest;
import com.acme.insurance.v1.RetryCoverageWorkflowResponse;
import com.acme.ledger.v1.JournalEntryNotificationServiceGrpc;
import com.acme.ledger.v1.SendJournalEntryTestNoticeRequest;
import com.acme.ledger.v1.SendJournalEntryTestNoticeResponse;
import com.acme.ledger.v1.UnsubscribeJournalEntryUpdatesRequest;
import com.acme.ledger.v1.UnsubscribeJournalEntryUpdatesResponse;
import com.acme.suppliers.v1.BatchCreateSuppliersRequest;
import com.acme.suppliers.v1.BatchCreateSuppliersResponse;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Runs the long-lived workflow that takes a payment promise from start to finish. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class PaymentPromiseWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(PaymentPromiseWorkflowApplication.class);

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final ChannelRegistry channels;

    private final CollectionCaseApprovalServiceGrpc.CollectionCaseApprovalServiceBlockingStub collectionsCollectionCaseApprovalServiceBlocking;

    private final CollectionCaseQueryServiceGrpc.CollectionCaseQueryServiceBlockingStub collectionsCollectionCaseQueryServiceBlocking;

    private final CollectionCaseServiceGrpc.CollectionCaseServiceBlockingStub collectionsCollectionCaseServiceBlocking;

    private final PaymentPromiseQueryServiceGrpc.PaymentPromiseQueryServiceBlockingStub collectionsPaymentPromiseQueryServiceBlocking;

    private final WriteOffApprovalServiceGrpc.WriteOffApprovalServiceBlockingStub collectionsWriteOffApprovalServiceBlocking;

    private final WriteOffValidationServiceGrpc.WriteOffValidationServiceBlockingStub collectionsWriteOffValidationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final CoverageReportingServiceGrpc.CoverageReportingServiceBlockingStub insuranceCoverageReportingServiceBlocking;

    private final CoverageWorkflowServiceGrpc.CoverageWorkflowServiceBlockingStub insuranceCoverageWorkflowServiceBlocking;

    private final PremiumApprovalServiceGrpc.PremiumApprovalServiceBlockingStub insurancePremiumApprovalServiceBlocking;

    private final JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceBlockingStub ledgerJournalEntryNotificationServiceBlocking;

    private final CollectionsRepository repository;

    private final SupplierServiceGrpc.SupplierServiceStub suppliersSupplierServiceAsync;

    public PaymentPromiseWorkflowApplication(
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            ChannelRegistry channels,
            CollectionCaseApprovalServiceGrpc.CollectionCaseApprovalServiceBlockingStub collectionsCollectionCaseApprovalServiceBlocking,
            CollectionCaseQueryServiceGrpc.CollectionCaseQueryServiceBlockingStub collectionsCollectionCaseQueryServiceBlocking,
            CollectionCaseServiceGrpc.CollectionCaseServiceBlockingStub collectionsCollectionCaseServiceBlocking,
            PaymentPromiseQueryServiceGrpc.PaymentPromiseQueryServiceBlockingStub collectionsPaymentPromiseQueryServiceBlocking,
            WriteOffApprovalServiceGrpc.WriteOffApprovalServiceBlockingStub collectionsWriteOffApprovalServiceBlocking,
            WriteOffValidationServiceGrpc.WriteOffValidationServiceBlockingStub collectionsWriteOffValidationServiceBlocking,
            ApplicationEventPublisher events,
            CoverageReportingServiceGrpc.CoverageReportingServiceBlockingStub insuranceCoverageReportingServiceBlocking,
            CoverageWorkflowServiceGrpc.CoverageWorkflowServiceBlockingStub insuranceCoverageWorkflowServiceBlocking,
            PremiumApprovalServiceGrpc.PremiumApprovalServiceBlockingStub insurancePremiumApprovalServiceBlocking,
            JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceBlockingStub ledgerJournalEntryNotificationServiceBlocking,
            CollectionsRepository repository,
            SupplierServiceGrpc.SupplierServiceStub suppliersSupplierServiceAsync) {
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.channels = channels;
        this.collectionsCollectionCaseApprovalServiceBlocking = collectionsCollectionCaseApprovalServiceBlocking;
        this.collectionsCollectionCaseQueryServiceBlocking = collectionsCollectionCaseQueryServiceBlocking;
        this.collectionsCollectionCaseServiceBlocking = collectionsCollectionCaseServiceBlocking;
        this.collectionsPaymentPromiseQueryServiceBlocking = collectionsPaymentPromiseQueryServiceBlocking;
        this.collectionsWriteOffApprovalServiceBlocking = collectionsWriteOffApprovalServiceBlocking;
        this.collectionsWriteOffValidationServiceBlocking = collectionsWriteOffValidationServiceBlocking;
        this.events = events;
        this.insuranceCoverageReportingServiceBlocking = insuranceCoverageReportingServiceBlocking;
        this.insuranceCoverageWorkflowServiceBlocking = insuranceCoverageWorkflowServiceBlocking;
        this.insurancePremiumApprovalServiceBlocking = insurancePremiumApprovalServiceBlocking;
        this.ledgerJournalEntryNotificationServiceBlocking = ledgerJournalEntryNotificationServiceBlocking;
        this.repository = repository;
        this.suppliersSupplierServiceAsync = suppliersSupplierServiceAsync;
    }

    public record InspectPaymentPromiseWorkflowStage0Requested(SendJournalEntryTestNoticeRequest request, CompletableFuture<SendJournalEntryTestNoticeResponse> completion, io.grpc.Context context) {}

    /**
     * Starts a payment promise.
     */
    public StartPaymentPromiseWorkflowResponse startPaymentPromiseWorkflow(StartPaymentPromiseWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartPaymentPromiseWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePaymentPromise(request.getContext().getTenantId(), request.getPaymentPromise());

        var outbound0 = RejectWriteOffRequest.newBuilder()
                .setContext(request.getContext())
                .setWriteOff(WriteOff.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getPaymentPromise().getAudit().getCreatedBy())
                                .build())
                        .setCounterparty(CollectionCaseRef.newBuilder()
                                .setDisplayName(request.getPaymentPromise().getOrganizer().getDisplayName())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.collections.v1.WriteOffApprovalService/RejectWriteOff", outbound0);
        var reply0 = collectionsWriteOffApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectWriteOff(command0.payload());

        var results1 = new ArrayList<CountCoverageTotalsResponse>();
        for (var element : request.getPaymentPromise().getParticipantsList()) {
            CountCoverageTotalsRequest itemRequest = CountCoverageTotalsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountCoverageTotalsRequest.Filter.newBuilder()
                        .setOwnerId(element.getParticipantId())
                        .setRegion(request.getPaymentPromise().getLocation().getRegion())
                        .build())
                .build();
            results1.add(insuranceCoverageReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countCoverageTotals(itemRequest));
        }
        var reply1 = results1.isEmpty() ? CountCoverageTotalsResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = RecallWriteOffRequest.newBuilder()
                .setContext(request.getContext())
                .setWriteOff(WriteOff.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply0.getWriteOff().getBillingAddress().getRegion())
                                .build())
                        .setCard(WriteOffCardSettlement.newBuilder()
                                .setLast4(switch (reply0.getWriteOff().getSettlementCase()) {
                                    case CARD -> reply0.getWriteOff().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply2 = collectionsWriteOffApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallWriteOff(outbound2);

        var outbound3 = BatchCreateSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateSuppliersRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getNotify())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(BatchCreateSuppliersResponse.getDefaultInstance());
        var input3 = suppliersSupplierServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateSuppliers(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();

        var outbound4 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getPaymentPromise().getAudit().getCreatedAt())
                                .setCreatedBy(request.getPaymentPromise().getAudit().getCreatedBy())
                                .setEtag(request.getPaymentPromise().getAudit().getEtag())
                                .setUpdatedAt(request.getPaymentPromise().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input4 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();
        log.atDebug().addKeyValue("auditValue", request.getPaymentPromise().getPaymentPromiseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartPaymentPromiseWorkflow", request.getPaymentPromise().getPaymentPromiseId());
        return StartPaymentPromiseWorkflowResponse.newBuilder()
                .setOutcome(StartPaymentPromiseWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getWriteOff().getAudit().getCreatedBy())
                                .build())
                        .setLocation(request.getPaymentPromise().getLocation())
                        .setPaymentPromiseId(request.getPaymentPromiseId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Advances a payment promise.
     */
    public AdvancePaymentPromiseWorkflowResponse advancePaymentPromiseWorkflow(AdvancePaymentPromiseWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvancePaymentPromiseWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePaymentPromise(request.getContext().getTenantId(), request.getPaymentPromise());

        var outbound0 = CloneCollectionCaseRequest.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getPaymentPromise().getOrganizer().getUserId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = collectionsCollectionCaseServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneCollectionCase(outbound0);

        var outbound1 = SendJournalEntryTestNoticeRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SendJournalEntryTestNoticeRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(reply0.getCollectionCase().getAssignee().getEmail())
                                .setName(request.getDetails().getNotify().getName())
                                .setPreferredChannel(reply0.getCollectionCase().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.ledger.v1.JournalEntryNotificationService/SendJournalEntryTestNotice", outbound1);
        var reply1 = ledgerJournalEntryNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).sendJournalEntryTestNotice(command1.payload());

        var outbound2 = ResolveCollectionCasesRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var reply2 = Optional.of(outbound2).map(value -> value.toBuilder().build())
                .map(value -> collectionsCollectionCaseQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveCollectionCases(value)).orElseThrow();

        var outbound3 = SubmitWriteOffRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubmitWriteOffRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getPaymentPromise().getOrganizer().getDisplayName())
                                .build())
                        .build())
                .setWriteOff(WriteOff.newBuilder()
                        .setBank(WriteOffBankSettlement.newBuilder()
                                .setBic(switch (reply1.getJournalEntry().getSettlementCase()) {
                                    case BANK -> reply1.getJournalEntry().getBank().getBic();
                                    default -> "";
                                })
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getPaymentPromise().getLocation().getLinesList())
                                .build())
                        .build())
                .build();
        var reply3 = collectionsWriteOffApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitWriteOff(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getPaymentPromise().getPaymentPromiseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvancePaymentPromiseWorkflow", request.getPaymentPromise().getPaymentPromiseId());
        return AdvancePaymentPromiseWorkflowResponse.newBuilder()
                .setOutcome(AdvancePaymentPromiseWorkflowResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply3.getOutcome().getNextStepsList())
                        .build())
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getJournalEntry().getAudit().getCreatedBy())
                                .setUpdatedAt(reply0.getCollectionCase().getAudit().getUpdatedAt())
                                .build())
                        .setLocation(request.getPaymentPromise().getLocation())
                        .putAllMetadata(reply3.getWriteOff().getMetadataMap())
                        .setOrganizer(UserRef.newBuilder()
                                .setUserId(reply0.getCollectionCase().getAssignee().getUserId())
                                .build())
                        .setPaymentPromiseId(request.getPaymentPromiseId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Cancels a payment promise.
     */
    public CancelPaymentPromiseWorkflowResponse cancelPaymentPromiseWorkflow(CancelPaymentPromiseWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelPaymentPromiseWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePaymentPromise(request.getContext().getTenantId(), request.getPaymentPromise());

        var outbound0 = ExportCollectionCasesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportCollectionCasesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getPaymentPromise().getAudit().getUpdatedAt())
                        .putAllLabels(request.getDetails().getParametersMap())
                        .setRegion(request.getPaymentPromise().getLocation().getRegion())
                        .build())
                .build();
        var stream0 = collectionsCollectionCaseQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportCollectionCases(outbound0);
        var reply0 = ExportCollectionCasesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = SubmitCollectionCaseRequest.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .addComments(CollectionCaseComment.newBuilder()
                                .setCreatedAt(request.getPaymentPromise().getAudit().getCreatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getPaymentPromise().getOrganizer().getEmail())
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var command1 = new DispatchCommand<>("acme.collections.v1.CollectionCaseApprovalService/SubmitCollectionCase", outbound1);
        var reply1 = collectionsCollectionCaseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitCollectionCase(command1.payload());

        var outbound2 = RpcSupport.unpack(Any.pack(DelegatePremiumApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(DelegatePremiumApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getCollectionCase().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getPaymentPromise().getLocation().getLinesList())
                                .build())
                        .build())
                .build()), DelegatePremiumApprovalRequest.class);
        var reply2 = insurancePremiumApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).delegatePremiumApproval(outbound2);

        var outbound3 = BrowsePaymentPromisesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowsePaymentPromisesRequest.Filter.newBuilder()
                        .setRegion(request.getPaymentPromise().getLocation().getRegion())
                        .build())
                .build();
        var reply3 = collectionsPaymentPromiseQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browsePaymentPromises(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getPaymentPromise().getPaymentPromiseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelPaymentPromiseWorkflow", request.getPaymentPromise().getPaymentPromiseId());
        return CancelPaymentPromiseWorkflowResponse.newBuilder()
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setPostalCode(reply2.getPremium().getBillingAddress().getPostalCode())
                                .build())
                        .addParticipants(PaymentPromiseParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setPreferredChannel(reply1.getCollectionCase().getRequester().getPreferredChannel())
                                        .build())
                                .build())
                        .setPaymentPromiseId(request.getPaymentPromiseId())
                        .setTitle(request.getPaymentPromise().getTitle())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply3.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Retrys a payment promise.
     */
    public RetryPaymentPromiseWorkflowResponse retryPaymentPromiseWorkflow(RetryPaymentPromiseWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryPaymentPromiseWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePaymentPromise(request.getContext().getTenantId(), request.getPaymentPromise());

        var builder0 = UnsubscribeJournalEntryUpdatesRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDetails(UnsubscribeJournalEntryUpdatesRequest.Details.newBuilder()
                .setPayload(Any.pack(request.getContext()))
                .setRecipient(ContactInfo.newBuilder()
                        .setPhone(request.getDetails().getNotify().getPhone())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = ledgerJournalEntryNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).unsubscribeJournalEntryUpdates(outbound0);

        var outbound1 = SubmitWriteOffRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubmitWriteOffRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getPaymentPromise().getOrganizer().getDisplayName())
                                .build())
                        .build())
                .setWriteOff(WriteOff.newBuilder()
                        .setBank(WriteOffBankSettlement.newBuilder()
                                .setAccountHolder(switch (reply0.getJournalEntry().getSettlementCase()) {
                                    case BANK -> reply0.getJournalEntry().getBank().getAccountHolder();
                                    default -> "";
                                })
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply0.getJournalEntry().getBillingAddress().getLocality())
                                .build())
                        .build())
                .build();
        var reply1 = collectionsWriteOffApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitWriteOff(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPaymentPromise().getPaymentPromiseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryPaymentPromiseWorkflow", request.getPaymentPromise().getPaymentPromiseId());
        return RetryPaymentPromiseWorkflowResponse.newBuilder()
                .setOutcome(RetryPaymentPromiseWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .addAllLines(reply0.getJournalEntry().getBillingAddress().getLinesList())
                                .build())
                        .setOrganizer(request.getPaymentPromise().getOrganizer())
                        .setPaymentPromiseId(request.getPaymentPromiseId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Resumes a payment promise.
     */
    public ResumePaymentPromiseWorkflowResponse resumePaymentPromiseWorkflow(ResumePaymentPromiseWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumePaymentPromiseWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePaymentPromise(request.getContext().getTenantId(), request.getPaymentPromise());

        var outbound0 = RecallWriteOffRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecallWriteOffRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setUserId(request.getPaymentPromise().getOrganizer().getUserId())
                                .build())
                        .build())
                .setWriteOff(WriteOff.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getPaymentPromise().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getPaymentPromise().getLocation().getLinesList())
                                .build())
                        .build())
                .build();
        var reply0 = collectionsWriteOffApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallWriteOff(outbound0);

        var outbound1 = CompareFormOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setForm(Form.newBuilder()
                        .setBank(FormBankSettlement.newBuilder()
                                .setIban(reply0.getWriteOff().getBank().getIban())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.forms.v1.FormCalculationService");
        var serviceName1 = "acme.forms.v1" + "." + "FormCalculationService";
        var methodName1 = "Com" + "pareFormOptions";
        var descriptor1 = MethodDescriptor.<CompareFormOptionsRequest, CompareFormOptionsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(CompareFormOptionsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(CompareFormOptionsResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPaymentPromise().getPaymentPromiseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumePaymentPromiseWorkflow", request.getPaymentPromise().getPaymentPromiseId());
        return ResumePaymentPromiseWorkflowResponse.newBuilder()
                .setOutcome(ResumePaymentPromiseWorkflowResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                        .build())
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .setOrganizer(UserRef.newBuilder()
                                .setDisplayName(reply0.getWriteOff().getCounterparty().getDisplayName())
                                .build())
                        .setPaymentPromiseId(request.getPaymentPromiseId())
                        .setStatus(request.getPaymentPromise().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a payment promise.
     */
    public InspectPaymentPromiseWorkflowResponse inspectPaymentPromiseWorkflow(InspectPaymentPromiseWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectPaymentPromiseWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PaymentPromise", request.getPaymentPromiseId(), "InspectPaymentPromiseWorkflow");

        var outbound0 = SendJournalEntryTestNoticeRequest.newBuilder()
                .putAllAnnotations(request.getContext().getBaggageMap())
                .setContext(request.getContext())
                .setDetails(SendJournalEntryTestNoticeRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<SendJournalEntryTestNoticeResponse>();
        events.publishEvent(new InspectPaymentPromiseWorkflowStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();
        return InspectPaymentPromiseWorkflowResponse.newBuilder()
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getJournalEntry().getAudit().getUpdatedBy())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(reply0.getJournalEntry().getBillingAddress().getRegion())
                                .build())
                        .setPaymentPromiseId(request.getPaymentPromiseId())
                        .build())
                .build();
    }

    /**
     * Lists payment promises a page at a time.
     */
    public ListPaymentPromiseWorkflowsResponse listPaymentPromiseWorkflows(ListPaymentPromiseWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPaymentPromiseWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PaymentPromise", request.getContext().getTraceId(), "ListPaymentPromiseWorkflows");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPaymentPromiseWorkflows", request.getPage().getPageToken());
        return ListPaymentPromiseWorkflowsResponse.getDefaultInstance();
    }

    /**
     * Signals a payment promise.
     */
    public SignalPaymentPromiseWorkflowResponse signalPaymentPromiseWorkflow(SignalPaymentPromiseWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalPaymentPromiseWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePaymentPromise(request.getContext().getTenantId(), request.getPaymentPromise());

        var outbound0 = NormalizeWriteOffRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeWriteOffRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply0 = collectionsWriteOffValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeWriteOff(outbound0);

        var outbound1 = RetryCoverageWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getWriteOff().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.insurance.v1.CoverageWorkflowService/RetryCoverageWorkflow", outbound1);
        var reply1 = insuranceCoverageWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryCoverageWorkflow(command1.payload());
        log.atDebug().addKeyValue("auditValue", request.getPaymentPromise().getPaymentPromiseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalPaymentPromiseWorkflow", request.getPaymentPromise().getPaymentPromiseId());
        return SignalPaymentPromiseWorkflowResponse.newBuilder()
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getCoverage().getAudit().getCreatedAt())
                                .setEtag(reply1.getCoverage().getAudit().getEtag())
                                .build())
                        .setPaymentPromiseId(request.getPaymentPromiseId())
                        .setWindow(request.getPaymentPromise().getWindow())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }
}
