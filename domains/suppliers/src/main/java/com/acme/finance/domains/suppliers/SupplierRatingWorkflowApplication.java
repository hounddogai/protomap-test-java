package com.acme.finance.domains.suppliers;

import com.acme.audiences.v1.AudienceQueryServiceGrpc;
import com.acme.audiences.v1.CountAudiencesRequest;
import com.acme.audiences.v1.CountAudiencesResponse;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineBankSettlement;
import com.acme.budgeting.v1.BudgetLineLifecycleServiceGrpc;
import com.acme.budgeting.v1.ScheduleBudgetLineExpiryRequest;
import com.acme.budgeting.v1.ScheduleBudgetLineExpiryResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.Contract;
import com.acme.contracts.v1.ContractApprovalServiceGrpc;
import com.acme.contracts.v1.ContractCardSettlement;
import com.acme.contracts.v1.EscalateContractRequest;
import com.acme.contracts.v1.EscalateContractResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.LintCustomersRequest;
import com.acme.customers.v1.LintCustomersResponse;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.PreferenceDiscountAction;
import com.acme.customers.v1.QuotePreferenceRequest;
import com.acme.customers.v1.QuotePreferenceResponse;
import com.acme.customers.v1.ValidateConsentRecordRequest;
import com.acme.customers.v1.ValidateConsentRecordResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.fleet.v1.ListVehiclesRequest;
import com.acme.fleet.v1.ListVehiclesResponse;
import com.acme.fleet.v1.VehicleServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryLine;
import com.acme.ledger.v1.JournalEntryValidationServiceGrpc;
import com.acme.ledger.v1.NormalizeJournalEntryRequest;
import com.acme.ledger.v1.NormalizeJournalEntryResponse;
import com.acme.suppliers.v1.AdvanceSupplierRatingWorkflowRequest;
import com.acme.suppliers.v1.AdvanceSupplierRatingWorkflowResponse;
import com.acme.suppliers.v1.CancelSupplierRatingWorkflowRequest;
import com.acme.suppliers.v1.CancelSupplierRatingWorkflowResponse;
import com.acme.suppliers.v1.CheckSupplierRatingEligibilityRequest;
import com.acme.suppliers.v1.CheckSupplierRatingEligibilityResponse;
import com.acme.suppliers.v1.ExplainSupplierRatingDecisionRequest;
import com.acme.suppliers.v1.ExplainSupplierRatingDecisionResponse;
import com.acme.suppliers.v1.ExportSupplierContractsRequest;
import com.acme.suppliers.v1.ExportSupplierContractsResponse;
import com.acme.suppliers.v1.InspectSupplierRatingWorkflowRequest;
import com.acme.suppliers.v1.InspectSupplierRatingWorkflowResponse;
import com.acme.suppliers.v1.ListSupplierRatingWorkflowsRequest;
import com.acme.suppliers.v1.ListSupplierRatingWorkflowsResponse;
import com.acme.suppliers.v1.ListSuppliersRequest;
import com.acme.suppliers.v1.ListSuppliersResponse;
import com.acme.suppliers.v1.ResumeSupplierRatingWorkflowRequest;
import com.acme.suppliers.v1.ResumeSupplierRatingWorkflowResponse;
import com.acme.suppliers.v1.RetrySupplierRatingWorkflowRequest;
import com.acme.suppliers.v1.RetrySupplierRatingWorkflowResponse;
import com.acme.suppliers.v1.SignalSupplierRatingWorkflowRequest;
import com.acme.suppliers.v1.SignalSupplierRatingWorkflowResponse;
import com.acme.suppliers.v1.SignalSupplierWorkflowRequest;
import com.acme.suppliers.v1.SignalSupplierWorkflowResponse;
import com.acme.suppliers.v1.StartSupplierRatingWorkflowRequest;
import com.acme.suppliers.v1.StartSupplierRatingWorkflowResponse;
import com.acme.suppliers.v1.SuggestSupplierContractsRequest;
import com.acme.suppliers.v1.SuggestSupplierContractsResponse;
import com.acme.suppliers.v1.Supplier;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingCardSettlement;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.acme.suppliers.v1.SupplierWorkflowServiceGrpc;
import com.google.protobuf.Any;
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
 * Runs the long-lived workflow that takes a supplier rating from start to finish. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class SupplierRatingWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(SupplierRatingWorkflowApplication.class);

    private final AudienceQueryServiceGrpc.AudienceQueryServiceBlockingStub audiencesAudienceQueryServiceBlocking;

    private final BudgetLineLifecycleServiceGrpc.BudgetLineLifecycleServiceBlockingStub budgetingBudgetLineLifecycleServiceBlocking;

    private final ChannelRegistry channels;

    private final ContractApprovalServiceGrpc.ContractApprovalServiceBlockingStub contractsContractApprovalServiceBlocking;

    private final CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking;

    private final PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking;

    private final VehicleServiceGrpc.VehicleServiceBlockingStub fleetVehicleServiceBlocking;

    private final JournalEntryValidationServiceGrpc.JournalEntryValidationServiceBlockingStub ledgerJournalEntryValidationServiceBlocking;

    private final SupplierRatingWorkflowRequestMapper mapper;

    private final SuppliersRepository repository;

    private final Executor rpcExecutor;

    private final SupplierContractQueryServiceGrpc.SupplierContractQueryServiceBlockingStub suppliersSupplierContractQueryServiceBlocking;

    private final SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking;

    private final SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking;

    private final SupplierWorkflowServiceGrpc.SupplierWorkflowServiceBlockingStub suppliersSupplierWorkflowServiceBlocking;

    public SupplierRatingWorkflowApplication(
            AudienceQueryServiceGrpc.AudienceQueryServiceBlockingStub audiencesAudienceQueryServiceBlocking,
            BudgetLineLifecycleServiceGrpc.BudgetLineLifecycleServiceBlockingStub budgetingBudgetLineLifecycleServiceBlocking,
            ChannelRegistry channels,
            ContractApprovalServiceGrpc.ContractApprovalServiceBlockingStub contractsContractApprovalServiceBlocking,
            CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking,
            PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking,
            VehicleServiceGrpc.VehicleServiceBlockingStub fleetVehicleServiceBlocking,
            JournalEntryValidationServiceGrpc.JournalEntryValidationServiceBlockingStub ledgerJournalEntryValidationServiceBlocking,
            SupplierRatingWorkflowRequestMapper mapper,
            SuppliersRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierContractQueryServiceGrpc.SupplierContractQueryServiceBlockingStub suppliersSupplierContractQueryServiceBlocking,
            SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking,
            SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking,
            SupplierWorkflowServiceGrpc.SupplierWorkflowServiceBlockingStub suppliersSupplierWorkflowServiceBlocking) {
        this.audiencesAudienceQueryServiceBlocking = audiencesAudienceQueryServiceBlocking;
        this.budgetingBudgetLineLifecycleServiceBlocking = budgetingBudgetLineLifecycleServiceBlocking;
        this.channels = channels;
        this.contractsContractApprovalServiceBlocking = contractsContractApprovalServiceBlocking;
        this.customersCustomerValidationServiceBlocking = customersCustomerValidationServiceBlocking;
        this.customersPreferenceCalculationServiceBlocking = customersPreferenceCalculationServiceBlocking;
        this.fleetVehicleServiceBlocking = fleetVehicleServiceBlocking;
        this.ledgerJournalEntryValidationServiceBlocking = ledgerJournalEntryValidationServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierContractQueryServiceBlocking = suppliersSupplierContractQueryServiceBlocking;
        this.suppliersSupplierRatingValidationServiceBlocking = suppliersSupplierRatingValidationServiceBlocking;
        this.suppliersSupplierServiceBlocking = suppliersSupplierServiceBlocking;
        this.suppliersSupplierWorkflowServiceBlocking = suppliersSupplierWorkflowServiceBlocking;
    }

    /**
     * Starts a supplier rating.
     */
    public StartSupplierRatingWorkflowResponse startSupplierRatingWorkflow(StartSupplierRatingWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartSupplierRatingWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierRating(request.getContext().getTenantId(), request.getSupplierRating());

        var outbound0 = ExplainSupplierRatingDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainSupplierRatingDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getSupplierRating().getAudit().getCreatedBy())
                                .build())
                        .setCard(SupplierRatingCardSettlement.newBuilder()
                                .setHolderName(request.getSupplierRating().getCard().getHolderName())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> suppliersSupplierRatingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainSupplierRatingDecision(outbound0), rpcExecutor);

        var outbound1 = CheckSupplierRatingEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getSupplierRating().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> suppliersSupplierRatingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkSupplierRatingEligibility(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getSupplierRating().getSupplierRatingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartSupplierRatingWorkflow", request.getSupplierRating().getSupplierRatingId());
        return StartSupplierRatingWorkflowResponse.newBuilder()
                .setOutcome(StartSupplierRatingWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply0.getSupplierRating().getBillingAddress().getRegion())
                                .build())
                        .setNotes(request.getSupplierRating().getNotes())
                        .setSupplierRatingId(request.getSupplierRatingId())
                        .build())
                .build();
    }

    /**
     * Advances a supplier rating.
     */
    public AdvanceSupplierRatingWorkflowResponse advanceSupplierRatingWorkflow(AdvanceSupplierRatingWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceSupplierRatingWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierRating(request.getContext().getTenantId(), request.getSupplierRating());

        List<LintCustomersResponse> results0 = request.getSupplierRating().getLinesList().stream()
                .map(element -> LintCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .putAllAttributes(element.getAttributesMap())
                        .build())
                .build())
                .flatMap(value -> Stream.of(customersCustomerValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintCustomers(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? LintCustomersResponse.getDefaultInstance()
                : results0.getLast();
        log.atDebug().addKeyValue("auditValue", request.getSupplierRating().getSupplierRatingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceSupplierRatingWorkflow", request.getSupplierRating().getSupplierRatingId());
        return AdvanceSupplierRatingWorkflowResponse.newBuilder()
                .setSupplierRating(SupplierRating.newBuilder()
                        .setBillingAddress(request.getSupplierRating().getBillingAddress())
                        .setSupplierRatingId(request.getSupplierRatingId())
                        .build())
                .build();
    }

    /**
     * Cancels a supplier rating.
     */
    public CancelSupplierRatingWorkflowResponse cancelSupplierRatingWorkflow(CancelSupplierRatingWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelSupplierRatingWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierRating(request.getContext().getTenantId(), request.getSupplierRating());

        var outbound0 = ValidateConsentRecordRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .build())
                .setContext(request.getContext())
                .setDetails(ValidateConsentRecordRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getSupplierRating().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.customers.v1.ConsentRecordValidationService");
        var selected0 = ConsentRecordValidationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.validateConsentRecord(outbound0);

        var outbound1 = ListVehiclesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListVehiclesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getSupplierRating().getAudit().getUpdatedAt())
                        .setOwnerId(reply0.getResult().getCorrelationId())
                        .setRegion(request.getSupplierRating().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply1 = fleetVehicleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listVehicles(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getSupplierRating().getSupplierRatingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelSupplierRatingWorkflow", request.getSupplierRating().getSupplierRatingId());
        return CancelSupplierRatingWorkflowResponse.newBuilder()
                .setOutcome(CancelSupplierRatingWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setNotes(request.getSupplierRating().getNotes())
                        .setSupplierRatingId(request.getSupplierRatingId())
                        .build())
                .build();
    }

    /**
     * Retrys a supplier rating.
     */
    public RetrySupplierRatingWorkflowResponse retrySupplierRatingWorkflow(RetrySupplierRatingWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetrySupplierRatingWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierRating(request.getContext().getTenantId(), request.getSupplierRating());

        var outbound0 = EscalateContractRequest.newBuilder()
                .setContext(request.getContext())
                .setContract(Contract.newBuilder()
                        .setCard(ContractCardSettlement.newBuilder()
                                .setLast4(switch (request.getSupplierRating().getSettlementCase()) {
                                    case CARD -> request.getSupplierRating().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setDetails(EscalateContractRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getSupplierRating().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply0 = contractsContractApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateContract(outbound0);

        var outbound1 = mapper.retrySupplierRatingWorkflowStage1(request, reply0);
        var reply1 = suppliersSupplierServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listSuppliers(outbound1);

        List<QuotePreferenceResponse> results2 = request.getSupplierRating().getLinesList().stream()
                .map(element -> QuotePreferenceRequest.newBuilder()
                .setContext(request.getContext())
                .setPreference(Preference.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getSupplierRating().getAudit().getUpdatedBy())
                                .build())
                        .setDescription(element.getDescription())
                        .setDiscount(PreferenceDiscountAction.newBuilder()
                                .setAmount(element.getTax().getAmount())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(customersPreferenceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePreference(value)))
                .collect(Collectors.toList());
        var reply2 = results2.isEmpty() ? QuotePreferenceResponse.getDefaultInstance()
                : results2.getLast();

        var outbound3 = ScheduleBudgetLineExpiryRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply2.getPreference().getAudit().getUpdatedAt())
                                .setUpdatedBy(reply0.getContract().getAudit().getUpdatedBy())
                                .build())
                        .setBank(BudgetLineBankSettlement.newBuilder()
                                .setAccountHolder(reply0.getContract().getBank().getAccountHolder())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ScheduleBudgetLineExpiryRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getSupplierRating().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var reply3 = budgetingBudgetLineLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleBudgetLineExpiry(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getSupplierRating().getSupplierRatingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetrySupplierRatingWorkflow", request.getSupplierRating().getSupplierRatingId());
        return RetrySupplierRatingWorkflowResponse.newBuilder()
                .setOutcome(RetrySupplierRatingWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply0.getContract().getBillingAddress().getRecipient())
                                .build())
                        .setNumber(request.getSupplierRating().getNumber())
                        .setSupplierRatingId(request.getSupplierRatingId())
                        .setTotal(reply3.getBudgetLine().getTotal())
                        .build())
                .build();
    }

    /**
     * Resumes a supplier rating.
     */
    public ResumeSupplierRatingWorkflowResponse resumeSupplierRatingWorkflow(ResumeSupplierRatingWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeSupplierRatingWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierRating(request.getContext().getTenantId(), request.getSupplierRating());

        var outbound0 = SignalSupplierWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplier(Supplier.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getSupplierRating().getAudit().getEtag())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getSupplierRating().getCounterparty().getEmail())
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> suppliersSupplierWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).signalSupplierWorkflow(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getSupplierRating().getSupplierRatingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeSupplierRatingWorkflow", request.getSupplierRating().getSupplierRatingId());
        return ResumeSupplierRatingWorkflowResponse.newBuilder()
                .setSupplierRating(SupplierRating.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getSupplier().getPrimaryAddress().getLinesList())
                                .build())
                        .setNotes(request.getSupplierRating().getNotes())
                        .setSupplierRatingId(request.getSupplierRatingId())
                        .build())
                .build();
    }

    /**
     * Returns a supplier rating.
     */
    public InspectSupplierRatingWorkflowResponse inspectSupplierRatingWorkflow(InspectSupplierRatingWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectSupplierRatingWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "SupplierRating", request.getSupplierRatingId(), "InspectSupplierRatingWorkflow");

        var outbound0 = NormalizeJournalEntryRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeJournalEntryRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setJournalEntry(JournalEntry.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .addLines(JournalEntryLine.newBuilder()
                                .putAllAttributes(request.getContext().getBaggageMap())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(ledgerJournalEntryValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeJournalEntry(value)))
                .collect(Collectors.toList()).getFirst();
        return InspectSupplierRatingWorkflowResponse.newBuilder()
                .setSupplierRating(SupplierRating.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply0.getJournalEntry().getBillingAddress().getRecipient())
                                .build())
                        .setSupplierRatingId(request.getSupplierRatingId())
                        .build())
                .build();
    }

    /**
     * Lists supplier ratings a page at a time.
     */
    public ListSupplierRatingWorkflowsResponse listSupplierRatingWorkflows(ListSupplierRatingWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListSupplierRatingWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "SupplierRating", request.getContext().getTraceId(), "ListSupplierRatingWorkflows");

        var outbound0 = ExportSupplierContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportSupplierContractsRequest.Filter.newBuilder()
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .build())
                .build();
        var stream0 = suppliersSupplierContractQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportSupplierContracts(outbound0);
        var reply0 = ExportSupplierContractsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = JsonCodec.roundTrip(SuggestSupplierContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestSupplierContractsRequest.Filter.newBuilder()
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .setQuery(request.getFilter().getQuery())
                        .build())
                .setPage(PageRequest.newBuilder()
                        .setPageSize(request.getPage().getPageSize())
                        .setPageToken(request.getPage().getPageToken())
                        .build())
                .build(), SuggestSupplierContractsRequest.class);
        var reply1 = suppliersSupplierContractQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestSupplierContracts(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getOwnerId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListSupplierRatingWorkflows", request.getFilter().getOwnerId());
        return ListSupplierRatingWorkflowsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Signals a supplier rating.
     */
    public SignalSupplierRatingWorkflowResponse signalSupplierRatingWorkflow(SignalSupplierRatingWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalSupplierRatingWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierRating(request.getContext().getTenantId(), request.getSupplierRating());

        var outbound0 = JsonCodec.roundTrip(CountAudiencesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountAudiencesRequest.Filter.newBuilder()
                        .setRegion(request.getSupplierRating().getBillingAddress().getRegion())
                        .build())
                .build(), CountAudiencesRequest.class);
        var reply0 = audiencesAudienceQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countAudiences(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getSupplierRating().getSupplierRatingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalSupplierRatingWorkflow", request.getSupplierRating().getSupplierRatingId());
        return SignalSupplierRatingWorkflowResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setDueAt(request.getSupplierRating().getDueAt())
                        .setSupplierRatingId(request.getSupplierRatingId())
                        .build())
                .build();
    }
}
