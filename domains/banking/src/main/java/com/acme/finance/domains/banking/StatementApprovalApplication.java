package com.acme.finance.domains.banking;

import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.banking.v1.ApproveStatementRequest;
import com.acme.banking.v1.ApproveStatementResponse;
import com.acme.banking.v1.CommentStatementApprovalRequest;
import com.acme.banking.v1.CommentStatementApprovalResponse;
import com.acme.banking.v1.CompareMandateOptionsRequest;
import com.acme.banking.v1.CompareMandateOptionsResponse;
import com.acme.banking.v1.EscalateStatementRequest;
import com.acme.banking.v1.EscalateStatementResponse;
import com.acme.banking.v1.ExplainStandingOrderDecisionRequest;
import com.acme.banking.v1.ExplainStandingOrderDecisionResponse;
import com.acme.banking.v1.GetStatementApprovalStatusRequest;
import com.acme.banking.v1.GetStatementApprovalStatusResponse;
import com.acme.banking.v1.LintStandingOrdersRequest;
import com.acme.banking.v1.LintStandingOrdersResponse;
import com.acme.banking.v1.ListPendingStatementsRequest;
import com.acme.banking.v1.ListPendingStatementsResponse;
import com.acme.banking.v1.LookupStatementRequest;
import com.acme.banking.v1.LookupStatementResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.banking.v1.MandateCardSettlement;
import com.acme.banking.v1.MandateValidationServiceGrpc;
import com.acme.banking.v1.NormalizeStandingOrderRequest;
import com.acme.banking.v1.NormalizeStandingOrderResponse;
import com.acme.banking.v1.PreviewStandingOrderRequest;
import com.acme.banking.v1.PreviewStandingOrderResponse;
import com.acme.banking.v1.RecallStatementRequest;
import com.acme.banking.v1.RecallStatementResponse;
import com.acme.banking.v1.RejectStatementRequest;
import com.acme.banking.v1.RejectStatementResponse;
import com.acme.banking.v1.ScoreStandingOrderRequest;
import com.acme.banking.v1.ScoreStandingOrderResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StandingOrderRef;
import com.acme.banking.v1.StandingOrderValidationServiceGrpc;
import com.acme.banking.v1.Statement;
import com.acme.banking.v1.StatementQueryServiceGrpc;
import com.acme.banking.v1.SubmitStatementRequest;
import com.acme.banking.v1.SubmitStatementResponse;
import com.acme.banking.v1.SummarizeStatementsRequest;
import com.acme.banking.v1.SummarizeStatementsResponse;
import com.acme.banking.v1.ValidateMandateRequest;
import com.acme.banking.v1.ValidateMandateResponse;
import com.acme.banking.v1.ValidateStandingOrderRequest;
import com.acme.banking.v1.ValidateStandingOrderResponse;
import com.acme.certificates.v1.CertificateServiceGrpc;
import com.acme.certificates.v1.ListCertificatesRequest;
import com.acme.certificates.v1.ListCertificatesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordCalculationServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.EstimateConsentRecordRequest;
import com.acme.customers.v1.EstimateConsentRecordResponse;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.customers.v1.PreviewPreferenceRequest;
import com.acme.customers.v1.PreviewPreferenceResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ClaimRef;
import com.acme.insurance.v1.ImportUnderwritingsRequest;
import com.acme.insurance.v1.ImportUnderwritingsResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingImportServiceGrpc;
import com.acme.ledger.v1.UnsubscribeJournalEntryUpdatesRequest;
import com.acme.ledger.v1.UnsubscribeJournalEntryUpdatesResponse;
import com.acme.procurement.v1.BatchCreatePurchaseRequisitionsRequest;
import com.acme.procurement.v1.BatchCreatePurchaseRequisitionsResponse;
import com.acme.procurement.v1.PurchaseRequisitionServiceGrpc;
import com.google.protobuf.Any;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Routes statements through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class StatementApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(StatementApprovalApplication.class);

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking;

    private final MandateValidationServiceGrpc.MandateValidationServiceBlockingStub bankingMandateValidationServiceBlocking;

    private final StandingOrderValidationServiceGrpc.StandingOrderValidationServiceBlockingStub bankingStandingOrderValidationServiceBlocking;

    private final StatementQueryServiceGrpc.StatementQueryServiceBlockingStub bankingStatementQueryServiceBlocking;

    private final CertificateServiceGrpc.CertificateServiceBlockingStub certificatesCertificateServiceBlocking;

    private final ConsentRecordCalculationServiceGrpc.ConsentRecordCalculationServiceBlockingStub customersConsentRecordCalculationServiceBlocking;

    private final PreferenceValidationServiceGrpc.PreferenceValidationServiceBlockingStub customersPreferenceValidationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final UnderwritingImportServiceGrpc.UnderwritingImportServiceStub insuranceUnderwritingImportServiceAsync;

    private final PurchaseRequisitionServiceGrpc.PurchaseRequisitionServiceStub procurementPurchaseRequisitionServiceAsync;

    private final BankingRepository repository;

    public StatementApprovalApplication(
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking,
            MandateValidationServiceGrpc.MandateValidationServiceBlockingStub bankingMandateValidationServiceBlocking,
            StandingOrderValidationServiceGrpc.StandingOrderValidationServiceBlockingStub bankingStandingOrderValidationServiceBlocking,
            StatementQueryServiceGrpc.StatementQueryServiceBlockingStub bankingStatementQueryServiceBlocking,
            CertificateServiceGrpc.CertificateServiceBlockingStub certificatesCertificateServiceBlocking,
            ConsentRecordCalculationServiceGrpc.ConsentRecordCalculationServiceBlockingStub customersConsentRecordCalculationServiceBlocking,
            PreferenceValidationServiceGrpc.PreferenceValidationServiceBlockingStub customersPreferenceValidationServiceBlocking,
            ApplicationEventPublisher events,
            UnderwritingImportServiceGrpc.UnderwritingImportServiceStub insuranceUnderwritingImportServiceAsync,
            PurchaseRequisitionServiceGrpc.PurchaseRequisitionServiceStub procurementPurchaseRequisitionServiceAsync,
            BankingRepository repository) {
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.bankingMandateCalculationServiceBlocking = bankingMandateCalculationServiceBlocking;
        this.bankingMandateValidationServiceBlocking = bankingMandateValidationServiceBlocking;
        this.bankingStandingOrderValidationServiceBlocking = bankingStandingOrderValidationServiceBlocking;
        this.bankingStatementQueryServiceBlocking = bankingStatementQueryServiceBlocking;
        this.certificatesCertificateServiceBlocking = certificatesCertificateServiceBlocking;
        this.customersConsentRecordCalculationServiceBlocking = customersConsentRecordCalculationServiceBlocking;
        this.customersPreferenceValidationServiceBlocking = customersPreferenceValidationServiceBlocking;
        this.events = events;
        this.insuranceUnderwritingImportServiceAsync = insuranceUnderwritingImportServiceAsync;
        this.procurementPurchaseRequisitionServiceAsync = procurementPurchaseRequisitionServiceAsync;
        this.repository = repository;
    }

    public record SubmitStatementStage0Requested(UnsubscribeJournalEntryUpdatesRequest request, CompletableFuture<UnsubscribeJournalEntryUpdatesResponse> completion, io.grpc.Context context) {}

    /**
     * Submits a statement.
     */
    public SubmitStatementResponse submitStatement(SubmitStatementRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitStatement")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStatement(request.getContext().getTenantId(), request.getStatement());

        var outbound0 = UnsubscribeJournalEntryUpdatesRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(UnsubscribeJournalEntryUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .putAllVariables(request.getStatement().getMetadataMap())
                        .build())
                .setReason(request.getReason())
                .build();
        var completion0 = new CompletableFuture<UnsubscribeJournalEntryUpdatesResponse>();
        events.publishEvent(new SubmitStatementStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = BatchCreatePurchaseRequisitionsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreatePurchaseRequisitionsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector1 = new StreamCollector<>(BatchCreatePurchaseRequisitionsResponse.getDefaultInstance());
        var input1 = procurementPurchaseRequisitionServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreatePurchaseRequisitions(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();

        var outbound2 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getStatement().getAudit().getCreatedAt())
                                .setCreatedBy(request.getStatement().getAudit().getCreatedBy())
                                .setEtag(request.getStatement().getAudit().getEtag())
                                .setUpdatedAt(request.getStatement().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input2 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();
        log.atDebug().addKeyValue("auditValue", request.getStatement().getStatementId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitStatement", request.getStatement().getStatementId());
        return SubmitStatementResponse.newBuilder()
                .setStatement(Statement.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getJournalEntry().getBillingAddress().getCountryCode())
                                .build())
                        .putAllMetadata(request.getStatement().getMetadataMap())
                        .setStatementId(request.getStatementId())
                        .build())
                .build();
    }

    /**
     * Approves a statement.
     */
    public ApproveStatementResponse approveStatement(ApproveStatementRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveStatement")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStatement(request.getContext().getTenantId(), request.getStatement());

        var outbound0 = CompareMandateOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setCard(MandateCardSettlement.newBuilder()
                                .setToken(switch (request.getStatement().getSettlementCase()) {
                                    case CARD -> request.getStatement().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = bankingMandateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareMandateOptions(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getStatement().getStatementId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveStatement", request.getStatement().getStatementId());
        return ApproveStatementResponse.newBuilder()
                .setStatement(Statement.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getMandate().getBillingAddress().getPostalCode())
                                .build())
                        .setDueAt(request.getStatement().getDueAt())
                        .setStatementId(request.getStatementId())
                        .build())
                .build();
    }

    /**
     * Rejects a statement.
     */
    public RejectStatementResponse rejectStatement(RejectStatementRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectStatement")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStatement(request.getContext().getTenantId(), request.getStatement());

        var outbound0 = (LookupStatementRequest.newBuilder()
                .setContext(request.getContext())
                .setStatementId(request.getStatementId())
                .build()).toBuilder().build();
        var reply0 = bankingStatementQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lookupStatement(outbound0);

        var outbound1 = RpcSupport.unpack(Any.pack(PreviewPreferenceRequest.newBuilder()
                .setContext(request.getContext())
                .setPreference(Preference.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getStatement().getAudit().getCreatedAt())
                                .setCreatedBy(request.getStatement().getAudit().getCreatedBy())
                                .setUpdatedAt(reply0.getStatement().getAudit().getUpdatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getStatement().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build()), PreviewPreferenceRequest.class);
        var reply1 = customersPreferenceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewPreference(outbound1);

        var outbound2 = ImportUnderwritingsRequest.newBuilder()
                .setContext(request.getContext())
                .setRecord(Underwriting.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getStatement().getAudit().getCreatedAt())
                                .build())
                        .setCustomer(ClaimRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply1.getPreference().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(ImportUnderwritingsResponse.getDefaultInstance());
        var input2 = insuranceUnderwritingImportServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).importUnderwritings(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();
        log.atDebug().addKeyValue("auditValue", request.getStatement().getStatementId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectStatement", request.getStatement().getStatementId());
        return RejectStatementResponse.newBuilder()
                .setOutcome(RejectStatementResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setStatement(Statement.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getStatement().getAudit().getCreatedAt())
                                .setEtag(reply1.getPreference().getAudit().getEtag())
                                .setUpdatedAt(reply1.getPreference().getAudit().getUpdatedAt())
                                .build())
                        .setStatementId(request.getStatementId())
                        .build())
                .build();
    }

    /**
     * Escalates a statement.
     */
    public EscalateStatementResponse escalateStatement(EscalateStatementRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateStatement")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStatement(request.getContext().getTenantId(), request.getStatement());

        var outbound0 = ScoreStandingOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setStandingOrder(StandingOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getStatement().getAudit().getCreatedBy())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = bankingStandingOrderValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreStandingOrder(outbound0);

        var outbound1 = EstimateConsentRecordRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(HouseholdRef.newBuilder()
                                .setDisplayName(request.getStatement().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(EstimateConsentRecordRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(reply0.getStandingOrder().getLocation().getLocality())
                                .setPostalCode(request.getStatement().getBillingAddress().getPostalCode())
                                .setRegion(request.getStatement().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        var reply1 = customersConsentRecordCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateConsentRecord(outbound1);

        var outbound2 = RpcSupport.unpack(Any.pack(ValidateStandingOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setStandingOrder(StandingOrder.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setLocality(reply0.getStandingOrder().getLocation().getLocality())
                                .build())
                        .addParticipants(StandingOrderParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getDetails().getApprover().getEmail())
                                        .build())
                                .build())
                        .build())
                .build()), ValidateStandingOrderRequest.class);
        var reply2 = bankingStandingOrderValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateStandingOrder(outbound2);

        var outbound3 = LintStandingOrdersRequest.newBuilder()
                .setContext(request.getContext())
                .setStandingOrder(StandingOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply2.getStandingOrder().getAudit().getCreatedAt())
                                .build())
                        .addParticipants(StandingOrderParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getDetails().getApprover().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        Function<LintStandingOrdersRequest, LintStandingOrdersResponse> operation3 = bankingStandingOrderValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::lintStandingOrders;
        var reply3 = operation3.apply(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getStatement().getStatementId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateStatement", request.getStatement().getStatementId());
        return EscalateStatementResponse.newBuilder()
                .setOutcome(EscalateStatementResponse.Outcome.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setStatement(Statement.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply3.getStandingOrder().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply3.getStandingOrder().getLocation().getCountryCode())
                                .build())
                        .setCounterparty(StandingOrderRef.newBuilder()
                                .setDisplayName(reply2.getStandingOrder().getOrganizer().getDisplayName())
                                .build())
                        .putAllMetadata(request.getStatement().getMetadataMap())
                        .setStatementId(request.getStatementId())
                        .build())
                .build();
    }

    /**
     * Lists statements a page at a time.
     */
    public ListPendingStatementsResponse listPendingStatements(ListPendingStatementsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingStatements")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Statement", request.getContext().getTraceId(), "ListPendingStatements");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingStatements", request.getPage().getPageToken());
        return ListPendingStatementsResponse.getDefaultInstance();
    }

    /**
     * Recalls a statement.
     */
    public RecallStatementResponse recallStatement(RecallStatementRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallStatement")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStatement(request.getContext().getTenantId(), request.getStatement());

        var outbound0 = JsonCodec.roundTrip(PreviewStandingOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setStandingOrder(StandingOrder.newBuilder()
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build(), PreviewStandingOrderRequest.class);
        var reply0 = bankingStandingOrderValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewStandingOrder(outbound0);

        var outbound1 = NormalizeStandingOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setStandingOrder(StandingOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getStatement().getAudit().getCreatedBy())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getStandingOrder().getLocation().getCountryCode())
                                .setPostalCode(request.getStatement().getBillingAddress().getPostalCode())
                                .build())
                        .addParticipants(StandingOrderParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(reply0.getStandingOrder().getOrganizer().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        final class Stage1Adapter {
            NormalizeStandingOrderResponse fetch(NormalizeStandingOrderRequest input) {
                return bankingStandingOrderValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeStandingOrder(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);

        var outbound2 = ListCertificatesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListCertificatesRequest.Filter.newBuilder()
                        .setRegion(request.getStatement().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply2 = RpcSupport.call(outbound2, value -> certificatesCertificateServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listCertificates(value));

        var outbound3 = SummarizeStatementsRequest.newBuilder()
                .setContext(request.getContext())
                .setReason(request.getReason())
                .setStatementId(request.getStatement().getStatementId())
                .build();
        var reply3 = RpcSupport.call(outbound3, value -> bankingStatementQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeStatements(value));
        log.atDebug().addKeyValue("auditValue", request.getStatement().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallStatement", request.getStatement().getNumber());
        return RecallStatementResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .setStatement(Statement.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply1.getStandingOrder().getLocation().getPostalCode())
                                .build())
                        .setStatementId(request.getStatementId())
                        .setTotal(request.getStatement().getTotal())
                        .build())
                .build();
    }

    /**
     * Returns a statement.
     */
    public GetStatementApprovalStatusResponse getStatementApprovalStatus(GetStatementApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetStatementApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Statement", request.getStatementId(), "GetStatementApprovalStatus");

        var outbound0 = ValidateMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateMandateRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build();
        var adapter0 = new Function<ValidateMandateRequest, ValidateMandateResponse>() {
            @Override
            public ValidateMandateResponse apply(ValidateMandateRequest input) {
                return bankingMandateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateMandate(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);
        return GetStatementApprovalStatusResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setStatement(Statement.newBuilder()
                        .setStatementId(request.getStatementId())
                        .build())
                .build();
    }

    /**
     * Comments a statement.
     */
    public CommentStatementApprovalResponse commentStatementApproval(CommentStatementApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentStatementApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStatement(request.getContext().getTenantId(), request.getStatement());

        var outbound0 = BeanCopies.copy(ExplainStandingOrderDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setStandingOrder(StandingOrder.newBuilder()
                        .setOrganizer(UserRef.newBuilder()
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .build(), ExplainStandingOrderDecisionRequest.class);
        var reply0 = bankingStandingOrderValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainStandingOrderDecision(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getStatement().getStatementId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentStatementApproval", request.getStatement().getStatementId());
        return CommentStatementApprovalResponse.newBuilder()
                .setOutcome(CommentStatementApprovalResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setStatement(Statement.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getStandingOrder().getLocation().getCountryCode())
                                .setRecipient(reply0.getStandingOrder().getLocation().getRecipient())
                                .build())
                        .setStatementId(request.getStatementId())
                        .build())
                .build();
    }
}
