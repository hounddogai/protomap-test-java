package com.acme.finance.domains.contracts;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationApprovalServiceGrpc;
import com.acme.compliance.v1.CommentAttestationApprovalRequest;
import com.acme.compliance.v1.CommentAttestationApprovalResponse;
import com.acme.compliance.v1.DelegateAttestationApprovalRequest;
import com.acme.compliance.v1.DelegateAttestationApprovalResponse;
import com.acme.contracts.v1.ApproveContractClauseRequest;
import com.acme.contracts.v1.ApproveContractClauseResponse;
import com.acme.contracts.v1.BatchCreateContractsRequest;
import com.acme.contracts.v1.BatchCreateContractsResponse;
import com.acme.contracts.v1.BatchGetContractsRequest;
import com.acme.contracts.v1.BatchGetContractsResponse;
import com.acme.contracts.v1.CommentContractClauseApprovalRequest;
import com.acme.contracts.v1.CommentContractClauseApprovalResponse;
import com.acme.contracts.v1.Contract;
import com.acme.contracts.v1.ContractBankSettlement;
import com.acme.contracts.v1.ContractClause;
import com.acme.contracts.v1.ContractServiceGrpc;
import com.acme.contracts.v1.CreateContractRequest;
import com.acme.contracts.v1.CreateContractResponse;
import com.acme.contracts.v1.DelegateContractClauseApprovalRequest;
import com.acme.contracts.v1.DelegateContractClauseApprovalResponse;
import com.acme.contracts.v1.EscalateContractClauseRequest;
import com.acme.contracts.v1.EscalateContractClauseResponse;
import com.acme.contracts.v1.GetContractClauseApprovalStatusRequest;
import com.acme.contracts.v1.GetContractClauseApprovalStatusResponse;
import com.acme.contracts.v1.ListPendingContractClausesRequest;
import com.acme.contracts.v1.ListPendingContractClausesResponse;
import com.acme.contracts.v1.RejectContractClauseRequest;
import com.acme.contracts.v1.RejectContractClauseResponse;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.RenewalParticipant;
import com.acme.contracts.v1.RenewalRef;
import com.acme.contracts.v1.RenewalValidationServiceGrpc;
import com.acme.contracts.v1.ScoreRenewalRequest;
import com.acme.contracts.v1.ScoreRenewalResponse;
import com.acme.contracts.v1.SearchContractsRequest;
import com.acme.contracts.v1.SearchContractsResponse;
import com.acme.contracts.v1.SubmitContractClauseRequest;
import com.acme.contracts.v1.SubmitContractClauseResponse;
import com.acme.contracts.v1.WatchContractsRequest;
import com.acme.contracts.v1.WatchContractsResponse;
import com.acme.customers.v1.CalculateConsentRecordRequest;
import com.acme.customers.v1.CalculateConsentRecordResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerSegmentCondition;
import com.acme.customers.v1.CustomerSegmentDiscountAction;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.ExplainCustomerCalculationRequest;
import com.acme.customers.v1.ExplainCustomerCalculationResponse;
import com.acme.customers.v1.ExplainCustomerSegmentCalculationRequest;
import com.acme.customers.v1.ExplainCustomerSegmentCalculationResponse;
import com.acme.customers.v1.LintCustomersRequest;
import com.acme.customers.v1.LintCustomersResponse;
import com.acme.customers.v1.NormalizeConsentRecordRequest;
import com.acme.customers.v1.NormalizeConsentRecordResponse;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceDiscountAction;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.customers.v1.PreviewPreferenceRequest;
import com.acme.customers.v1.PreviewPreferenceResponse;
import com.acme.customers.v1.QuoteCustomerSegmentRequest;
import com.acme.customers.v1.QuoteCustomerSegmentResponse;
import com.acme.documents.v1.BatchGetDocumentsRequest;
import com.acme.documents.v1.BatchGetDocumentsResponse;
import com.acme.documents.v1.DocumentServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchLine;
import com.acme.ledger.v1.PostingBatchLineTax;
import com.acme.ledger.v1.PostingBatchValidationServiceGrpc;
import com.acme.ledger.v1.PreviewPostingBatchRequest;
import com.acme.ledger.v1.PreviewPostingBatchResponse;
import com.acme.venues.v1.ScoreVenueRequest;
import com.acme.venues.v1.ScoreVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueComponent;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Routes contract clauses through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class ContractClauseApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(ContractClauseApprovalApplication.class);

    private final AttestationApprovalServiceGrpc.AttestationApprovalServiceBlockingStub complianceAttestationApprovalServiceBlocking;

    private final ContractServiceGrpc.ContractServiceStub contractsContractServiceAsync;

    private final ContractServiceGrpc.ContractServiceBlockingStub contractsContractServiceBlocking;

    private final RenewalValidationServiceGrpc.RenewalValidationServiceBlockingStub contractsRenewalValidationServiceBlocking;

    private final ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceBlockingStub customersConsentRecordValidationServiceBlocking;

    private final CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking;

    private final CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking;

    private final CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking;

    private final PreferenceValidationServiceGrpc.PreferenceValidationServiceBlockingStub customersPreferenceValidationServiceBlocking;

    private final DocumentServiceGrpc.DocumentServiceBlockingStub documentsDocumentServiceBlocking;

    private final ApplicationEventPublisher events;

    private final PostingBatchValidationServiceGrpc.PostingBatchValidationServiceBlockingStub ledgerPostingBatchValidationServiceBlocking;

    private final ContractClauseApprovalRequestMapper mapper;

    private final ContractsRepository repository;

    private final VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking;

    public ContractClauseApprovalApplication(
            AttestationApprovalServiceGrpc.AttestationApprovalServiceBlockingStub complianceAttestationApprovalServiceBlocking,
            ContractServiceGrpc.ContractServiceStub contractsContractServiceAsync,
            ContractServiceGrpc.ContractServiceBlockingStub contractsContractServiceBlocking,
            RenewalValidationServiceGrpc.RenewalValidationServiceBlockingStub contractsRenewalValidationServiceBlocking,
            ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceBlockingStub customersConsentRecordValidationServiceBlocking,
            CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking,
            CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking,
            CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking,
            PreferenceValidationServiceGrpc.PreferenceValidationServiceBlockingStub customersPreferenceValidationServiceBlocking,
            DocumentServiceGrpc.DocumentServiceBlockingStub documentsDocumentServiceBlocking,
            ApplicationEventPublisher events,
            PostingBatchValidationServiceGrpc.PostingBatchValidationServiceBlockingStub ledgerPostingBatchValidationServiceBlocking,
            ContractClauseApprovalRequestMapper mapper,
            ContractsRepository repository,
            VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking) {
        this.complianceAttestationApprovalServiceBlocking = complianceAttestationApprovalServiceBlocking;
        this.contractsContractServiceAsync = contractsContractServiceAsync;
        this.contractsContractServiceBlocking = contractsContractServiceBlocking;
        this.contractsRenewalValidationServiceBlocking = contractsRenewalValidationServiceBlocking;
        this.customersConsentRecordValidationServiceBlocking = customersConsentRecordValidationServiceBlocking;
        this.customersCustomerCalculationServiceBlocking = customersCustomerCalculationServiceBlocking;
        this.customersCustomerSegmentCalculationServiceBlocking = customersCustomerSegmentCalculationServiceBlocking;
        this.customersCustomerValidationServiceBlocking = customersCustomerValidationServiceBlocking;
        this.customersPreferenceValidationServiceBlocking = customersPreferenceValidationServiceBlocking;
        this.documentsDocumentServiceBlocking = documentsDocumentServiceBlocking;
        this.events = events;
        this.ledgerPostingBatchValidationServiceBlocking = ledgerPostingBatchValidationServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.venuesVenueValidationServiceBlocking = venuesVenueValidationServiceBlocking;
    }

    public record ApproveContractClauseStage1Requested(CalculateConsentRecordRequest request, CompletableFuture<CalculateConsentRecordResponse> completion, io.grpc.Context context) {}

    /**
     * Submits a contract clause.
     */
    public SubmitContractClauseResponse submitContractClause(SubmitContractClauseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitContractClause")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveContractClause(request.getContext().getTenantId(), request.getContractClause());

        var outbound0 = JsonCodec.roundTrip(LintCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContractClause().getAudit().getCreatedAt())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContractClause().getOwner().getEmail())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build(), LintCustomersRequest.class);
        var reply0 = customersCustomerValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintCustomers(outbound0);

        var outbound1 = RpcSupport.unpack(Any.pack(NormalizeConsentRecordRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .build())
                .setContext(request.getContext())
                .setDetails(NormalizeConsentRecordRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContractClause().getOwner().getEmail())
                                .setName(reply0.getCustomer().getContact().getName())
                                .setPhone(reply0.getCustomer().getContact().getPhone())
                                .setPreferredChannel(reply0.getCustomer().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build()), NormalizeConsentRecordRequest.class);
        var reply1 = customersConsentRecordValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeConsentRecord(outbound1);

        var outbound2 = RpcSupport.unpack(Any.pack(BatchGetDocumentsRequest.newBuilder()
                .setContext(request.getContext())
                .build()), BatchGetDocumentsRequest.class);
        var reply2 = documentsDocumentServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetDocuments(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getContractClause().getContractClauseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitContractClause", request.getContractClause().getContractClauseId());
        return SubmitContractClauseResponse.newBuilder()
                .setContractClause(ContractClause.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getCustomer().getAudit().getEtag())
                                .setUpdatedAt(reply0.getCustomer().getAudit().getUpdatedAt())
                                .build())
                        .setContractClauseId(request.getContractClauseId())
                        .setEffective(request.getContractClause().getEffective())
                        .build())
                .setOutcome(SubmitContractClauseResponse.Outcome.newBuilder()
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Approves a contract clause.
     */
    public ApproveContractClauseResponse approveContractClause(ApproveContractClauseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveContractClause")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveContractClause(request.getContext().getTenantId(), request.getContractClause());

        var outbound0 = PreviewPostingBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setPostingBatch(PostingBatch.newBuilder()
                        .addLines(PostingBatchLine.newBuilder()
                                .setTax(PostingBatchLineTax.newBuilder()
                                        .setAmount(request.getContractClause().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = ledgerPostingBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewPostingBatch(outbound0);

        var outbound1 = CalculateConsentRecordRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .build())
                .setContext(request.getContext())
                .setDetails(CalculateConsentRecordRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getPostingBatch().getBillingAddress().getCountryCode())
                                .setLocality(reply0.getPostingBatch().getBillingAddress().getLocality())
                                .setRecipient(reply0.getPostingBatch().getBillingAddress().getRecipient())
                                .setRegion(reply0.getPostingBatch().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<CalculateConsentRecordResponse>();
        events.publishEvent(new ApproveContractClauseStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();

        var results2 = new ArrayList<QuoteCustomerSegmentResponse>();
        for (var element : request.getContractClause().getConditionsList()) {
            QuoteCustomerSegmentRequest itemRequest = QuoteCustomerSegmentRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .addConditions(CustomerSegmentCondition.newBuilder()
                                .setNegate(element.getNegate())
                                .setValue(element.getValue())
                                .build())
                        .build())
                .build();
            results2.add(customersCustomerSegmentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteCustomerSegment(itemRequest));
        }
        var reply2 = results2.isEmpty() ? QuoteCustomerSegmentResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getContractClause().getContractClauseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveContractClause", request.getContractClause().getContractClauseId());
        return ApproveContractClauseResponse.newBuilder()
                .setContractClause(ContractClause.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getPostingBatch().getAudit().getCreatedBy())
                                .build())
                        .setContractClauseId(request.getContractClauseId())
                        .setDescription(request.getContractClause().getDescription())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Rejects a contract clause.
     */
    public RejectContractClauseResponse rejectContractClause(RejectContractClauseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectContractClause")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveContractClause(request.getContext().getTenantId(), request.getContractClause());

        var outbound0 = JsonCodec.roundTrip(ExplainCustomerCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainCustomerCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContractClause().getOwner().getEmail())
                                .build())
                        .build())
                .build(), ExplainCustomerCalculationRequest.class);
        var reply0 = customersCustomerCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainCustomerCalculation(outbound0);

        var outbound1 = RpcSupport.unpack(Any.pack(ScoreVenueRequest.newBuilder()
                .setContext(request.getContext())
                .setVenue(Venue.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getCustomer().getPrimaryAddress().getPostalCode())
                                .build())
                        .build())
                .build()), ScoreVenueRequest.class);
        var reply1 = venuesVenueValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreVenue(outbound1);

        var outbound2 = mapper.rejectContractClauseStage2(request);
        var reply2 = customersCustomerSegmentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainCustomerSegmentCalculation(outbound2);

        var outbound3 = PreviewPreferenceRequest.newBuilder()
                .setContext(request.getContext())
                .setPreference(Preference.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getCustomer().getAudit().getCreatedBy())
                                .setUpdatedAt(reply1.getVenue().getAudit().getUpdatedAt())
                                .build())
                        .setDiscount(PreferenceDiscountAction.newBuilder()
                                .setAmount(request.getContractClause().getDiscount().getAmount())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContractClause().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        var adapter3 = new Function<PreviewPreferenceRequest, PreviewPreferenceResponse>() {
            @Override
            public PreviewPreferenceResponse apply(PreviewPreferenceRequest input) {
                return customersPreferenceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewPreference(input);
            }
        };
        var reply3 = adapter3.apply(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getContractClause().getContractClauseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectContractClause", request.getContractClause().getContractClauseId());
        return RejectContractClauseResponse.newBuilder()
                .setContractClause(ContractClause.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getCustomer().getAudit().getCreatedBy())
                                .setUpdatedBy(reply1.getVenue().getAudit().getUpdatedBy())
                                .build())
                        .setContractClauseId(request.getContractClauseId())
                        .setDescription(reply1.getVenue().getComponentsOrDefault("primary", VenueComponent.getDefaultInstance()).getDescription())
                        .setEffective(request.getContractClause().getEffective())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply2.getCustomerSegment().getOwner().getDisplayName())
                                .build())
                        .build())
                .setOutcome(RejectContractClauseResponse.Outcome.newBuilder()
                        .setStatus(reply3.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply3.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Escalates a contract clause.
     */
    public EscalateContractClauseResponse escalateContractClause(EscalateContractClauseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateContractClause")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveContractClause(request.getContext().getTenantId(), request.getContractClause());

        var outbound0 = SearchContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchContractsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContractClause().getAudit().getCreatedAt())
                        .setOwnerId(request.getDetails().getApprover().getUserId())
                        .build())
                .build();
        final class Stage0Adapter {
            SearchContractsResponse fetch(SearchContractsRequest input) {
                return contractsContractServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchContracts(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = WatchContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(WatchContractsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContractClause().getAudit().getCreatedAt())
                        .setOwnerId(request.getContractClause().getOwner().getUserId())
                        .build())
                .setSince(request.getContractClause().getAudit().getUpdatedAt())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getApprover().getEmail())
                        .build())
                .build();
        var stream1 = contractsContractServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchContracts(outbound1);
        var reply1 = WatchContractsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getContractClause().getContractClauseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateContractClause", request.getContractClause().getContractClauseId());
        return EscalateContractClauseResponse.newBuilder()
                .setContractClause(ContractClause.newBuilder()
                        .setContractClauseId(request.getContractClauseId())
                        .setEffective(request.getContractClause().getEffective())
                        .build())
                .setOutcome(EscalateContractClauseResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Lists contract clauses a page at a time.
     */
    public ListPendingContractClausesResponse listPendingContractClauses(ListPendingContractClausesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingContractClauses")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ContractClause", request.getContext().getTraceId(), "ListPendingContractClauses");

        var outbound0 = ScoreRenewalRequest.newBuilder()
                .setContext(request.getContext())
                .setRenewal(Renewal.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .addParticipants(RenewalParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getContext().getActor().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = contractsRenewalValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreRenewal(outbound0);

        var outbound1 = (BatchGetContractsRequest.newBuilder()
                .setContext(request.getContext())
                .build()).toBuilder().build();
        var reply1 = contractsContractServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetContracts(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingContractClauses", request.getPage().getPageToken());
        return ListPendingContractClausesResponse.newBuilder()
                .addContractClauses(ContractClause.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getRenewal().getAudit().getEtag())
                                .setUpdatedAt(reply0.getRenewal().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setPageInfo(PageInfo.newBuilder()
                        .setTotalSize(reply1.getPageInfo().getTotalSize())
                        .build())
                .build();
    }

    /**
     * Delegates a contract clause.
     */
    public DelegateContractClauseApprovalResponse delegateContractClauseApproval(DelegateContractClauseApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateContractClauseApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveContractClause(request.getContext().getTenantId(), request.getContractClause());

        var outbound0 = mapper.delegateContractClauseApprovalStage0(request);
        var reply0 = contractsContractServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createContract(outbound0);

        var outbound1 = BatchCreateContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setRecord(Contract.newBuilder()
                        .setBank(ContractBankSettlement.newBuilder()
                                .setIban(reply0.getContract().getBank().getIban())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getContract().getBillingAddress().getCountryCode())
                                .build())
                        .setCounterparty(RenewalRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .build();
        var collector1 = new StreamCollector<>(BatchCreateContractsResponse.getDefaultInstance());
        var input1 = contractsContractServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateContracts(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();
        log.atDebug().addKeyValue("auditValue", request.getContractClause().getContractClauseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateContractClauseApproval", request.getContractClause().getContractClauseId());
        return DelegateContractClauseApprovalResponse.newBuilder()
                .setContractClause(ContractClause.newBuilder()
                        .setContractClauseId(request.getContractClauseId())
                        .setEffective(request.getContractClause().getEffective())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Returns a contract clause.
     */
    public GetContractClauseApprovalStatusResponse getContractClauseApprovalStatus(GetContractClauseApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetContractClauseApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ContractClause", request.getContractClauseId(), "GetContractClauseApprovalStatus");

        var outbound0 = CommentAttestationApprovalRequest.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CommentAttestationApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> complianceAttestationApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentAttestationApproval(value)).orElseThrow();
        return GetContractClauseApprovalStatusResponse.newBuilder()
                .setContractClause(ContractClause.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getAttestation().getAudit().getCreatedAt())
                                .build())
                        .setContractClauseId(request.getContractClauseId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Comments a contract clause.
     */
    public CommentContractClauseApprovalResponse commentContractClauseApproval(CommentContractClauseApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentContractClauseApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveContractClause(request.getContext().getTenantId(), request.getContractClause());

        var outbound0 = BeanCopies.copy(DelegateAttestationApprovalRequest.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getContractClause().getAudit().getCreatedBy())
                                .setEtag(request.getContractClause().getAudit().getEtag())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(DelegateAttestationApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getContractClause().getOwner().getDisplayName())
                                .build())
                        .build())
                .setReason(request.getContractClause().getBlock().getReason())
                .build(), DelegateAttestationApprovalRequest.class);
        var reply0 = complianceAttestationApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).delegateAttestationApproval(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getContractClause().getContractClauseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentContractClauseApproval", request.getContractClause().getContractClauseId());
        return CommentContractClauseApprovalResponse.newBuilder()
                .setContractClause(ContractClause.newBuilder()
                        .setContractClauseId(request.getContractClauseId())
                        .setVersion(request.getContractClause().getVersion())
                        .build())
                .setOutcome(CommentContractClauseApprovalResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .build();
    }
}
