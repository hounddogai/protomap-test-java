package com.acme.finance.domains.suppliers;

import com.acme.assets.v1.SubmitAssetTransferRequest;
import com.acme.assets.v1.SubmitAssetTransferResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateApprovalServiceGrpc;
import com.acme.banking.v1.MandateAttachment;
import com.acme.banking.v1.RejectMandateRequest;
import com.acme.banking.v1.RejectMandateResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationApprovalServiceGrpc;
import com.acme.compliance.v1.AttestationCardSettlement;
import com.acme.compliance.v1.CommentAttestationApprovalRequest;
import com.acme.compliance.v1.CommentAttestationApprovalResponse;
import com.acme.contracts.v1.CommentContractClauseApprovalRequest;
import com.acme.contracts.v1.CommentContractClauseApprovalResponse;
import com.acme.contracts.v1.ContractClause;
import com.acme.contracts.v1.ContractClauseApprovalServiceGrpc;
import com.acme.contracts.v1.EscalateContractClauseRequest;
import com.acme.contracts.v1.EscalateContractClauseResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.PreviewCustomerRequest;
import com.acme.customers.v1.PreviewCustomerResponse;
import com.acme.customers.v1.QuoteHouseholdRequest;
import com.acme.customers.v1.QuoteHouseholdResponse;
import com.acme.customers.v1.ScoreConsentRecordRequest;
import com.acme.customers.v1.ScoreConsentRecordResponse;
import com.acme.customers.v1.ValidateCustomerSegmentRequest;
import com.acme.customers.v1.ValidateCustomerSegmentResponse;
import com.acme.employees.v1.EmployeeQueryServiceGrpc;
import com.acme.employees.v1.SummarizeEmployeesRequest;
import com.acme.employees.v1.SummarizeEmployeesResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.AnnotatePostingBatchVersionRequest;
import com.acme.ledger.v1.AnnotatePostingBatchVersionResponse;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchHistoryServiceGrpc;
import com.acme.ledger.v1.PostingBatchLine;
import com.acme.ledger.v1.UnsubscribeFiscalPeriodUpdatesRequest;
import com.acme.ledger.v1.UnsubscribeFiscalPeriodUpdatesResponse;
import com.acme.suppliers.v1.AdvanceSupplierWorkflowRequest;
import com.acme.suppliers.v1.AdvanceSupplierWorkflowResponse;
import com.acme.suppliers.v1.ApproveSupplierRatingRequest;
import com.acme.suppliers.v1.ApproveSupplierRatingResponse;
import com.acme.suppliers.v1.BrowseSupplierContractsRequest;
import com.acme.suppliers.v1.BrowseSupplierContractsResponse;
import com.acme.suppliers.v1.BrowseSuppliersRequest;
import com.acme.suppliers.v1.BrowseSuppliersResponse;
import com.acme.suppliers.v1.CancelSupplierWorkflowRequest;
import com.acme.suppliers.v1.CancelSupplierWorkflowResponse;
import com.acme.suppliers.v1.CommentSupplierRatingApprovalRequest;
import com.acme.suppliers.v1.CommentSupplierRatingApprovalResponse;
import com.acme.suppliers.v1.DelegateSupplierRatingApprovalRequest;
import com.acme.suppliers.v1.DelegateSupplierRatingApprovalResponse;
import com.acme.suppliers.v1.EscalateSupplierRatingRequest;
import com.acme.suppliers.v1.EscalateSupplierRatingResponse;
import com.acme.suppliers.v1.GetSupplierRatingApprovalStatusRequest;
import com.acme.suppliers.v1.GetSupplierRatingApprovalStatusResponse;
import com.acme.suppliers.v1.ListPendingSupplierRatingsRequest;
import com.acme.suppliers.v1.ListPendingSupplierRatingsResponse;
import com.acme.suppliers.v1.PreviewSupplierRatingRequest;
import com.acme.suppliers.v1.PreviewSupplierRatingResponse;
import com.acme.suppliers.v1.RecallSupplierContractRequest;
import com.acme.suppliers.v1.RecallSupplierContractResponse;
import com.acme.suppliers.v1.RecallSupplierRatingRequest;
import com.acme.suppliers.v1.RecallSupplierRatingResponse;
import com.acme.suppliers.v1.RejectSupplierRatingRequest;
import com.acme.suppliers.v1.RejectSupplierRatingResponse;
import com.acme.suppliers.v1.ResolveSupplierContractsRequest;
import com.acme.suppliers.v1.ResolveSupplierContractsResponse;
import com.acme.suppliers.v1.ResolveSuppliersRequest;
import com.acme.suppliers.v1.ResolveSuppliersResponse;
import com.acme.suppliers.v1.SubmitSupplierRatingRequest;
import com.acme.suppliers.v1.SubmitSupplierRatingResponse;
import com.acme.suppliers.v1.Supplier;
import com.acme.suppliers.v1.SupplierContract;
import com.acme.suppliers.v1.SupplierContractApprovalServiceGrpc;
import com.acme.suppliers.v1.SupplierContractLine;
import com.acme.suppliers.v1.SupplierContractLineTax;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.SupplierRiskValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierWorkflowServiceGrpc;
import com.acme.suppliers.v1.ValidateSupplierRiskRequest;
import com.acme.suppliers.v1.ValidateSupplierRiskResponse;
import com.google.protobuf.Any;
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
 * Routes supplier ratings through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class SupplierRatingApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(SupplierRatingApprovalApplication.class);

    private final MandateApprovalServiceGrpc.MandateApprovalServiceBlockingStub bankingMandateApprovalServiceBlocking;

    private final ChannelRegistry channels;

    private final AttestationApprovalServiceGrpc.AttestationApprovalServiceBlockingStub complianceAttestationApprovalServiceBlocking;

    private final ContractClauseApprovalServiceGrpc.ContractClauseApprovalServiceBlockingStub contractsContractClauseApprovalServiceBlocking;

    private final ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceBlockingStub customersConsentRecordValidationServiceBlocking;

    private final CustomerValidationServiceGrpc.CustomerValidationServiceFutureStub customersCustomerValidationServiceFuture;

    private final HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking;

    private final EmployeeQueryServiceGrpc.EmployeeQueryServiceBlockingStub employeesEmployeeQueryServiceBlocking;

    private final ApplicationEventPublisher events;

    private final FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceBlockingStub ledgerFiscalPeriodNotificationServiceBlocking;

    private final PostingBatchHistoryServiceGrpc.PostingBatchHistoryServiceBlockingStub ledgerPostingBatchHistoryServiceBlocking;

    private final SuppliersRepository repository;

    private final Executor rpcExecutor;

    private final SupplierContractApprovalServiceGrpc.SupplierContractApprovalServiceFutureStub suppliersSupplierContractApprovalServiceFuture;

    private final SupplierContractQueryServiceGrpc.SupplierContractQueryServiceBlockingStub suppliersSupplierContractQueryServiceBlocking;

    private final SupplierQueryServiceGrpc.SupplierQueryServiceBlockingStub suppliersSupplierQueryServiceBlocking;

    private final SupplierRiskValidationServiceGrpc.SupplierRiskValidationServiceBlockingStub suppliersSupplierRiskValidationServiceBlocking;

    private final SupplierWorkflowServiceGrpc.SupplierWorkflowServiceBlockingStub suppliersSupplierWorkflowServiceBlocking;

    public SupplierRatingApprovalApplication(
            MandateApprovalServiceGrpc.MandateApprovalServiceBlockingStub bankingMandateApprovalServiceBlocking,
            ChannelRegistry channels,
            AttestationApprovalServiceGrpc.AttestationApprovalServiceBlockingStub complianceAttestationApprovalServiceBlocking,
            ContractClauseApprovalServiceGrpc.ContractClauseApprovalServiceBlockingStub contractsContractClauseApprovalServiceBlocking,
            ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceBlockingStub customersConsentRecordValidationServiceBlocking,
            CustomerValidationServiceGrpc.CustomerValidationServiceFutureStub customersCustomerValidationServiceFuture,
            HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking,
            EmployeeQueryServiceGrpc.EmployeeQueryServiceBlockingStub employeesEmployeeQueryServiceBlocking,
            ApplicationEventPublisher events,
            FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceBlockingStub ledgerFiscalPeriodNotificationServiceBlocking,
            PostingBatchHistoryServiceGrpc.PostingBatchHistoryServiceBlockingStub ledgerPostingBatchHistoryServiceBlocking,
            SuppliersRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierContractApprovalServiceGrpc.SupplierContractApprovalServiceFutureStub suppliersSupplierContractApprovalServiceFuture,
            SupplierContractQueryServiceGrpc.SupplierContractQueryServiceBlockingStub suppliersSupplierContractQueryServiceBlocking,
            SupplierQueryServiceGrpc.SupplierQueryServiceBlockingStub suppliersSupplierQueryServiceBlocking,
            SupplierRiskValidationServiceGrpc.SupplierRiskValidationServiceBlockingStub suppliersSupplierRiskValidationServiceBlocking,
            SupplierWorkflowServiceGrpc.SupplierWorkflowServiceBlockingStub suppliersSupplierWorkflowServiceBlocking) {
        this.bankingMandateApprovalServiceBlocking = bankingMandateApprovalServiceBlocking;
        this.channels = channels;
        this.complianceAttestationApprovalServiceBlocking = complianceAttestationApprovalServiceBlocking;
        this.contractsContractClauseApprovalServiceBlocking = contractsContractClauseApprovalServiceBlocking;
        this.customersConsentRecordValidationServiceBlocking = customersConsentRecordValidationServiceBlocking;
        this.customersCustomerValidationServiceFuture = customersCustomerValidationServiceFuture;
        this.customersHouseholdCalculationServiceBlocking = customersHouseholdCalculationServiceBlocking;
        this.employeesEmployeeQueryServiceBlocking = employeesEmployeeQueryServiceBlocking;
        this.events = events;
        this.ledgerFiscalPeriodNotificationServiceBlocking = ledgerFiscalPeriodNotificationServiceBlocking;
        this.ledgerPostingBatchHistoryServiceBlocking = ledgerPostingBatchHistoryServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierContractApprovalServiceFuture = suppliersSupplierContractApprovalServiceFuture;
        this.suppliersSupplierContractQueryServiceBlocking = suppliersSupplierContractQueryServiceBlocking;
        this.suppliersSupplierQueryServiceBlocking = suppliersSupplierQueryServiceBlocking;
        this.suppliersSupplierRiskValidationServiceBlocking = suppliersSupplierRiskValidationServiceBlocking;
        this.suppliersSupplierWorkflowServiceBlocking = suppliersSupplierWorkflowServiceBlocking;
    }

    public record SubmitSupplierRatingStage3Requested(ResolveSupplierContractsRequest request, CompletableFuture<ResolveSupplierContractsResponse> completion, io.grpc.Context context) {}

    public record EscalateSupplierRatingStage0Requested(ValidateCustomerSegmentRequest request, CompletableFuture<ValidateCustomerSegmentResponse> completion, io.grpc.Context context) {}

    public record GetSupplierRatingApprovalStatusStage1Requested(SubmitAssetTransferRequest request, CompletableFuture<SubmitAssetTransferResponse> completion, io.grpc.Context context) {}

    /**
     * Submits a supplier rating.
     */
    public SubmitSupplierRatingResponse submitSupplierRating(SubmitSupplierRatingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitSupplierRating")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierRating(request.getContext().getTenantId(), request.getSupplierRating());

        var builder0 = UnsubscribeFiscalPeriodUpdatesRequest.newBuilder();
        builder0.putAllAnnotations(request.getAnnotationsMap());
        builder0.setContext(request.getContext());
        builder0.setDetails(UnsubscribeFiscalPeriodUpdatesRequest.Details.newBuilder()
                .setPayload(Any.pack(request.getContext()))
                .setRecipient(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getApprover().getEmail())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = ledgerFiscalPeriodNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).unsubscribeFiscalPeriodUpdates(outbound0);

        List<AnnotatePostingBatchVersionResponse> results1 = request.getSupplierRating().getLinesList().stream()
                .map(element -> AnnotatePostingBatchVersionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(AnnotatePostingBatchVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setPostingBatch(PostingBatch.newBuilder()
                        .addLines(PostingBatchLine.newBuilder()
                                .setAmount(element.getTax().getAmount())
                                .putAllAttributes(element.getAttributesMap())
                                .setUnitPrice(element.getUnitPrice())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(ledgerPostingBatchHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).annotatePostingBatchVersion(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? AnnotatePostingBatchVersionResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = ValidateSupplierRiskRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRisk(SupplierRisk.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getSupplierRating().getCounterparty().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getSupplierRating().getAudit().getCreatedAt())
                                .setUpdatedBy(reply0.getFiscalPeriod().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        RpcPort<ValidateSupplierRiskRequest, ValidateSupplierRiskResponse> port2 = RpcPort.adapt(suppliersSupplierRiskValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::validateSupplierRisk);
        var reply2 = port2.exchange(outbound2);

        var outbound3 = ResolveSupplierContractsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var completion3 = new CompletableFuture<ResolveSupplierContractsResponse>();
        events.publishEvent(new SubmitSupplierRatingStage3Requested(outbound3, completion3, io.grpc.Context.current()));
        var reply3 = completion3.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getSupplierRating().getSupplierRatingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitSupplierRating", request.getSupplierRating().getSupplierRatingId());
        return SubmitSupplierRatingResponse.newBuilder()
                .setOutcome(SubmitSupplierRatingResponse.Outcome.newBuilder()
                        .setStatus(reply3.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply3.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply2.getSupplierRisk().getRequester().getEmail())
                                .build())
                        .putAllMetadata(request.getSupplierRating().getMetadataMap())
                        .setSupplierRatingId(request.getSupplierRatingId())
                        .build())
                .build();
    }

    /**
     * Approves a supplier rating.
     */
    public ApproveSupplierRatingResponse approveSupplierRating(ApproveSupplierRatingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveSupplierRating")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierRating(request.getContext().getTenantId(), request.getSupplierRating());

        var outbound0 = QuoteHouseholdRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(request.getSupplierRating().getAudit().getCreatedAt())
                                                .build())
                                        .setLocation(PostalAddress.newBuilder()
                                                .addAllLines(request.getSupplierRating().getBillingAddress().getLinesList())
                                                .setRegion(request.getSupplierRating().getBillingAddress().getRegion())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(request.getSupplierRating().getCounterparty().getDisplayName())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> customersHouseholdCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteHousehold(value));

        var outbound1 = PreviewSupplierRatingRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewSupplierRatingRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getHousehold().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.suppliers.v1.SupplierRatingValidationService");
        var selected1 = SupplierRatingValidationServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.previewSupplierRating(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getSupplierRating().getSupplierRatingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveSupplierRating", request.getSupplierRating().getSupplierRatingId());
        return ApproveSupplierRatingResponse.newBuilder()
                .setOutcome(ApproveSupplierRatingResponse.Outcome.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getHousehold().getLocation().getCountryCode())
                                .addAllLines(reply0.getHousehold().getLocation().getLinesList())
                                .build())
                        .setNotes(request.getSupplierRating().getNotes())
                        .setSupplierRatingId(request.getSupplierRatingId())
                        .build())
                .build();
    }

    /**
     * Rejects a supplier rating.
     */
    public RejectSupplierRatingResponse rejectSupplierRating(RejectSupplierRatingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectSupplierRating")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierRating(request.getContext().getTenantId(), request.getSupplierRating());

        var futures0 = request.getSupplierRating().getAttachmentsList().stream()
                .map(element -> RejectMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .addAttachments(MandateAttachment.newBuilder()
                                .setContentType(element.getContentType())
                                .setFileName(element.getFileName())
                                .setUri(element.getUri())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> bankingMandateApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectMandate(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? RejectMandateResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = RpcSupport.unpack(Any.pack(EscalateContractClauseRequest.newBuilder()
                .setContext(request.getContext())
                .setContractClause(ContractClause.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .setEmail(reply0.getMandate().getCounterparty().getEmail())
                                .build())
                        .build())
                .setDetails(EscalateContractClauseRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .build()), EscalateContractClauseRequest.class);
        var reply1 = contractsContractClauseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateContractClause(outbound1);

        var outbound2 = SummarizeEmployeesRequest.newBuilder()
                .setContext(request.getContext())
                .setReason(switch (reply1.getContractClause().getActionCase()) {
                    case BLOCK -> reply1.getContractClause().getBlock().getReason();
                    default -> "";
                })
                .build();
        var reply2 = employeesEmployeeQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeEmployees(outbound2);

        var outbound3 = RecallSupplierContractRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierContract(SupplierContract.newBuilder()
                        .addLines(SupplierContractLine.newBuilder()
                                .setTax(SupplierContractLineTax.newBuilder()
                                        .setAmount(reply1.getContractClause().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var pending3 = suppliersSupplierContractApprovalServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).recallSupplierContract(outbound3);
        var reply3 = RpcSupport.await(pending3);
        log.atDebug().addKeyValue("auditValue", request.getSupplierRating().getSupplierRatingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectSupplierRating", request.getSupplierRating().getSupplierRatingId());
        return RejectSupplierRatingResponse.newBuilder()
                .setOutcome(RejectSupplierRatingResponse.Outcome.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply3.getSupplierContract().getBillingAddress().getRegion())
                                .build())
                        .putAllMetadata(request.getSupplierRating().getMetadataMap())
                        .setNotes(reply3.getSupplierContract().getNotes())
                        .setSupplierRatingId(request.getSupplierRatingId())
                        .build())
                .build();
    }

    /**
     * Escalates a supplier rating.
     */
    public EscalateSupplierRatingResponse escalateSupplierRating(EscalateSupplierRatingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateSupplierRating")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierRating(request.getContext().getTenantId(), request.getSupplierRating());

        var outbound0 = ValidateCustomerSegmentRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<ValidateCustomerSegmentResponse>();
        events.publishEvent(new EscalateSupplierRatingStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = CancelSupplierWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplier(Supplier.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getSupplierRating().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> suppliersSupplierWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cancelSupplierWorkflow(value));
        log.atDebug().addKeyValue("auditValue", request.getSupplierRating().getSupplierRatingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateSupplierRating", request.getSupplierRating().getSupplierRatingId());
        return EscalateSupplierRatingResponse.newBuilder()
                .setOutcome(EscalateSupplierRatingResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllNextSteps(reply1.getOutcome().getNextStepsList())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setIssuedAt(request.getSupplierRating().getIssuedAt())
                        .setSupplierRatingId(request.getSupplierRatingId())
                        .build())
                .build();
    }

    /**
     * Lists supplier ratings a page at a time.
     */
    public ListPendingSupplierRatingsResponse listPendingSupplierRatings(ListPendingSupplierRatingsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingSupplierRatings")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "SupplierRating", request.getContext().getTraceId(), "ListPendingSupplierRatings");

        var outbound0 = PreviewCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var pending0 = customersCustomerValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).previewCustomer(outbound0);
        var reply0 = RpcSupport.await(pending0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingSupplierRatings", request.getPage().getPageToken());
        return ListPendingSupplierRatingsResponse.newBuilder()
                .addSupplierRatings(SupplierRating.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply0.getCustomer().getContact().getEmail())
                                .build())
                        .build())
                .build();
    }

    /**
     * Delegates a supplier rating.
     */
    public DelegateSupplierRatingApprovalResponse delegateSupplierRatingApproval(DelegateSupplierRatingApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateSupplierRatingApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierRating(request.getContext().getTenantId(), request.getSupplierRating());

        var outbound0 = BrowseSupplierContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseSupplierContractsRequest.Filter.newBuilder()
                        .setRegion(request.getSupplierRating().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> suppliersSupplierContractQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseSupplierContracts(outbound0), rpcExecutor);

        var outbound1 = CommentAttestationApprovalRequest.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getSupplierRating().getBillingAddress().getLinesList())
                                .build())
                        .setCard(AttestationCardSettlement.newBuilder()
                                .setLast4(request.getSupplierRating().getCard().getLast4())
                                .setNetwork(request.getSupplierRating().getCard().getNetwork())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CommentAttestationApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> complianceAttestationApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentAttestationApproval(outbound1), rpcExecutor);

        var outbound2 = ResolveSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending2 = CompletableFuture.supplyAsync(() -> suppliersSupplierQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveSuppliers(outbound2), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1, pending2).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        var reply2 = pending2.join();
        log.atDebug().addKeyValue("auditValue", request.getSupplierRating().getSupplierRatingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateSupplierRatingApproval", request.getSupplierRating().getSupplierRatingId());
        return DelegateSupplierRatingApprovalResponse.newBuilder()
                .setOutcome(DelegateSupplierRatingApprovalResponse.Outcome.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .addAllNextSteps(reply1.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setStatus(request.getSupplierRating().getStatus())
                        .setSupplierRatingId(request.getSupplierRatingId())
                        .build())
                .build();
    }

    /**
     * Recalls a supplier rating.
     */
    public RecallSupplierRatingResponse recallSupplierRating(RecallSupplierRatingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallSupplierRating")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierRating(request.getContext().getTenantId(), request.getSupplierRating());

        var outbound0 = (CommentContractClauseApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setContractClause(ContractClause.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getSupplierRating().getAudit().getUpdatedAt())
                                .setUpdatedBy(request.getSupplierRating().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getSupplierRating().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setDetails(CommentContractClauseApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = contractsContractClauseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentContractClauseApproval(outbound0);

        var outbound1 = ScoreConsentRecordRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .build())
                .setContext(request.getContext())
                .setDetails(ScoreConsentRecordRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(request.getSupplierRating().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<ScoreConsentRecordRequest, ScoreConsentRecordResponse>() {
            @Override
            public ScoreConsentRecordResponse apply(ScoreConsentRecordRequest input) {
                return customersConsentRecordValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreConsentRecord(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getSupplierRating().getSupplierRatingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallSupplierRating", request.getSupplierRating().getSupplierRatingId());
        return RecallSupplierRatingResponse.newBuilder()
                .setOutcome(RecallSupplierRatingResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setDueAt(request.getSupplierRating().getDueAt())
                        .setSupplierRatingId(request.getSupplierRatingId())
                        .setTotal(reply1.getBreakdown().getTotal())
                        .build())
                .build();
    }

    /**
     * Returns a supplier rating.
     */
    public GetSupplierRatingApprovalStatusResponse getSupplierRatingApprovalStatus(GetSupplierRatingApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetSupplierRatingApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "SupplierRating", request.getSupplierRatingId(), "GetSupplierRatingApprovalStatus");

        var outbound0 = BrowseSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseSuppliersRequest.Filter.newBuilder()
                        .setOwnerId(request.getSupplierRatingId())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> suppliersSupplierQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseSuppliers(value)).orElseThrow();

        var outbound1 = SubmitAssetTransferRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubmitAssetTransferRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<SubmitAssetTransferResponse>();
        events.publishEvent(new GetSupplierRatingApprovalStatusStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();
        return GetSupplierRatingApprovalStatusResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setBillingAddress(reply1.getAssetTransfer().getBillingAddress())
                        .setSupplierRatingId(request.getSupplierRatingId())
                        .build())
                .build();
    }

    /**
     * Comments a supplier rating.
     */
    public CommentSupplierRatingApprovalResponse commentSupplierRatingApproval(CommentSupplierRatingApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentSupplierRatingApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierRating(request.getContext().getTenantId(), request.getSupplierRating());

        var outbound0 = AdvanceSupplierWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplier(Supplier.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getSupplierRating().getAudit().getCreatedAt())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getSupplierRating().getBillingAddress().getCountryCode())
                                .addAllLines(request.getSupplierRating().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<AdvanceSupplierWorkflowRequest, AdvanceSupplierWorkflowResponse>() {
            @Override
            public AdvanceSupplierWorkflowResponse apply(AdvanceSupplierWorkflowRequest input) {
                return suppliersSupplierWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).advanceSupplierWorkflow(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getSupplierRating().getSupplierRatingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentSupplierRatingApproval", request.getSupplierRating().getSupplierRatingId());
        return CommentSupplierRatingApprovalResponse.newBuilder()
                .setOutcome(CommentSupplierRatingApprovalResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply0.getSupplier().getRelationshipManager().getDisplayName())
                                .build())
                        .setSupplierRatingId(request.getSupplierRatingId())
                        .setTotal(request.getSupplierRating().getTotal())
                        .build())
                .build();
    }
}
