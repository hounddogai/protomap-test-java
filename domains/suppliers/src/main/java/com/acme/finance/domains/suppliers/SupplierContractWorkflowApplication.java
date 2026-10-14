package com.acme.finance.domains.suppliers;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailCardSettlement;
import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.audit.v1.SearchAuditEventsRequest;
import com.acme.audit.v1.SearchAuditEventsResponse;
import com.acme.audit.v1.ValidateAuditTrailRequest;
import com.acme.audit.v1.ValidateAuditTrailResponse;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineLine;
import com.acme.budgeting.v1.BudgetLineLineTax;
import com.acme.budgeting.v1.BudgetLineWorkflowServiceGrpc;
import com.acme.budgeting.v1.ResumeBudgetLineWorkflowRequest;
import com.acme.budgeting.v1.ResumeBudgetLineWorkflowResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CalculatePreferenceRequest;
import com.acme.customers.v1.CalculatePreferenceResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerServiceGrpc;
import com.acme.customers.v1.HouseholdQueryServiceGrpc;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.ListCustomersRequest;
import com.acme.customers.v1.ListCustomersResponse;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.QuotePreferenceRequest;
import com.acme.customers.v1.QuotePreferenceResponse;
import com.acme.customers.v1.RecalculateConsentRecordsRequest;
import com.acme.customers.v1.RecalculateConsentRecordsResponse;
import com.acme.customers.v1.ResolveHouseholdsRequest;
import com.acme.customers.v1.ResolveHouseholdsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.JournalEntryQueryServiceGrpc;
import com.acme.ledger.v1.ResolveJournalEntriesRequest;
import com.acme.ledger.v1.ResolveJournalEntriesResponse;
import com.acme.lending.v1.RepaymentReportingServiceGrpc;
import com.acme.lending.v1.StreamRepaymentMetricsRequest;
import com.acme.lending.v1.StreamRepaymentMetricsResponse;
import com.acme.lineage.v1.LineageNode;
import com.acme.lineage.v1.LineageNodeCalculationServiceGrpc;
import com.acme.lineage.v1.SimulateLineageNodeRequest;
import com.acme.lineage.v1.SimulateLineageNodeResponse;
import com.acme.suppliers.v1.AdvanceSupplierContractWorkflowRequest;
import com.acme.suppliers.v1.AdvanceSupplierContractWorkflowResponse;
import com.acme.suppliers.v1.CancelSupplierContractWorkflowRequest;
import com.acme.suppliers.v1.CancelSupplierContractWorkflowResponse;
import com.acme.suppliers.v1.CheckSupplierRatingEligibilityRequest;
import com.acme.suppliers.v1.CheckSupplierRatingEligibilityResponse;
import com.acme.suppliers.v1.CheckSupplierRiskEligibilityRequest;
import com.acme.suppliers.v1.CheckSupplierRiskEligibilityResponse;
import com.acme.suppliers.v1.CreateSupplierRequest;
import com.acme.suppliers.v1.CreateSupplierResponse;
import com.acme.suppliers.v1.DelegateSupplierRatingApprovalRequest;
import com.acme.suppliers.v1.DelegateSupplierRatingApprovalResponse;
import com.acme.suppliers.v1.ExportSupplierContractsRequest;
import com.acme.suppliers.v1.ExportSupplierContractsResponse;
import com.acme.suppliers.v1.FindSupplierContractsRequest;
import com.acme.suppliers.v1.FindSupplierContractsResponse;
import com.acme.suppliers.v1.InspectSupplierContractWorkflowRequest;
import com.acme.suppliers.v1.InspectSupplierContractWorkflowResponse;
import com.acme.suppliers.v1.ListSupplierContractWorkflowsRequest;
import com.acme.suppliers.v1.ListSupplierContractWorkflowsResponse;
import com.acme.suppliers.v1.NormalizeSupplierRatingRequest;
import com.acme.suppliers.v1.NormalizeSupplierRatingResponse;
import com.acme.suppliers.v1.NormalizeSupplierRiskRequest;
import com.acme.suppliers.v1.NormalizeSupplierRiskResponse;
import com.acme.suppliers.v1.PauseSupplierContractWorkflowRequest;
import com.acme.suppliers.v1.PauseSupplierContractWorkflowResponse;
import com.acme.suppliers.v1.ResolveSupplierContractsRequest;
import com.acme.suppliers.v1.ResolveSupplierContractsResponse;
import com.acme.suppliers.v1.ResumeSupplierContractWorkflowRequest;
import com.acme.suppliers.v1.ResumeSupplierContractWorkflowResponse;
import com.acme.suppliers.v1.RetrySupplierContractWorkflowRequest;
import com.acme.suppliers.v1.RetrySupplierContractWorkflowResponse;
import com.acme.suppliers.v1.RetrySupplierWorkflowRequest;
import com.acme.suppliers.v1.RetrySupplierWorkflowResponse;
import com.acme.suppliers.v1.SignalSupplierContractWorkflowRequest;
import com.acme.suppliers.v1.SignalSupplierContractWorkflowResponse;
import com.acme.suppliers.v1.StartSupplierContractWorkflowRequest;
import com.acme.suppliers.v1.StartSupplierContractWorkflowResponse;
import com.acme.suppliers.v1.Supplier;
import com.acme.suppliers.v1.SupplierContract;
import com.acme.suppliers.v1.SupplierContractLine;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingApprovalServiceGrpc;
import com.acme.suppliers.v1.SupplierRatingBankSettlement;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.SupplierRiskValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.acme.suppliers.v1.SupplierWorkflowServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
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
 * Runs the long-lived workflow that takes a supplier contract from start to finish. Coordinates validation,
 * durable audit writes, and downstream operations.
 */
@Service
public class SupplierContractWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(SupplierContractWorkflowApplication.class);

    private final AuditEventServiceGrpc.AuditEventServiceBlockingStub auditAuditEventServiceBlocking;

    private final AuditTrailValidationServiceGrpc.AuditTrailValidationServiceBlockingStub auditAuditTrailValidationServiceBlocking;

    private final BudgetLineWorkflowServiceGrpc.BudgetLineWorkflowServiceBlockingStub budgetingBudgetLineWorkflowServiceBlocking;

    private final ChannelRegistry channels;

    private final CustomerServiceGrpc.CustomerServiceBlockingStub customersCustomerServiceBlocking;

    private final HouseholdQueryServiceGrpc.HouseholdQueryServiceBlockingStub customersHouseholdQueryServiceBlocking;

    private final PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking;

    private final JournalEntryQueryServiceGrpc.JournalEntryQueryServiceBlockingStub ledgerJournalEntryQueryServiceBlocking;

    private final RepaymentReportingServiceGrpc.RepaymentReportingServiceBlockingStub lendingRepaymentReportingServiceBlocking;

    private final LineageNodeCalculationServiceGrpc.LineageNodeCalculationServiceBlockingStub lineageLineageNodeCalculationServiceBlocking;

    private final SupplierContractWorkflowRequestMapper mapper;

    private final SuppliersRepository repository;

    private final Executor rpcExecutor;

    private final SupplierContractQueryServiceGrpc.SupplierContractQueryServiceBlockingStub suppliersSupplierContractQueryServiceBlocking;

    private final SupplierRatingApprovalServiceGrpc.SupplierRatingApprovalServiceBlockingStub suppliersSupplierRatingApprovalServiceBlocking;

    private final SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking;

    private final SupplierRiskValidationServiceGrpc.SupplierRiskValidationServiceBlockingStub suppliersSupplierRiskValidationServiceBlocking;

    private final SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking;

    private final SupplierWorkflowServiceGrpc.SupplierWorkflowServiceBlockingStub suppliersSupplierWorkflowServiceBlocking;

    public SupplierContractWorkflowApplication(
            AuditEventServiceGrpc.AuditEventServiceBlockingStub auditAuditEventServiceBlocking,
            AuditTrailValidationServiceGrpc.AuditTrailValidationServiceBlockingStub auditAuditTrailValidationServiceBlocking,
            BudgetLineWorkflowServiceGrpc.BudgetLineWorkflowServiceBlockingStub budgetingBudgetLineWorkflowServiceBlocking,
            ChannelRegistry channels,
            CustomerServiceGrpc.CustomerServiceBlockingStub customersCustomerServiceBlocking,
            HouseholdQueryServiceGrpc.HouseholdQueryServiceBlockingStub customersHouseholdQueryServiceBlocking,
            PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking,
            JournalEntryQueryServiceGrpc.JournalEntryQueryServiceBlockingStub ledgerJournalEntryQueryServiceBlocking,
            RepaymentReportingServiceGrpc.RepaymentReportingServiceBlockingStub lendingRepaymentReportingServiceBlocking,
            LineageNodeCalculationServiceGrpc.LineageNodeCalculationServiceBlockingStub lineageLineageNodeCalculationServiceBlocking,
            SupplierContractWorkflowRequestMapper mapper,
            SuppliersRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierContractQueryServiceGrpc.SupplierContractQueryServiceBlockingStub suppliersSupplierContractQueryServiceBlocking,
            SupplierRatingApprovalServiceGrpc.SupplierRatingApprovalServiceBlockingStub suppliersSupplierRatingApprovalServiceBlocking,
            SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking,
            SupplierRiskValidationServiceGrpc.SupplierRiskValidationServiceBlockingStub suppliersSupplierRiskValidationServiceBlocking,
            SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking,
            SupplierWorkflowServiceGrpc.SupplierWorkflowServiceBlockingStub suppliersSupplierWorkflowServiceBlocking) {
        this.auditAuditEventServiceBlocking = auditAuditEventServiceBlocking;
        this.auditAuditTrailValidationServiceBlocking = auditAuditTrailValidationServiceBlocking;
        this.budgetingBudgetLineWorkflowServiceBlocking = budgetingBudgetLineWorkflowServiceBlocking;
        this.channels = channels;
        this.customersCustomerServiceBlocking = customersCustomerServiceBlocking;
        this.customersHouseholdQueryServiceBlocking = customersHouseholdQueryServiceBlocking;
        this.customersPreferenceCalculationServiceBlocking = customersPreferenceCalculationServiceBlocking;
        this.ledgerJournalEntryQueryServiceBlocking = ledgerJournalEntryQueryServiceBlocking;
        this.lendingRepaymentReportingServiceBlocking = lendingRepaymentReportingServiceBlocking;
        this.lineageLineageNodeCalculationServiceBlocking = lineageLineageNodeCalculationServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierContractQueryServiceBlocking = suppliersSupplierContractQueryServiceBlocking;
        this.suppliersSupplierRatingApprovalServiceBlocking = suppliersSupplierRatingApprovalServiceBlocking;
        this.suppliersSupplierRatingValidationServiceBlocking = suppliersSupplierRatingValidationServiceBlocking;
        this.suppliersSupplierRiskValidationServiceBlocking = suppliersSupplierRiskValidationServiceBlocking;
        this.suppliersSupplierServiceBlocking = suppliersSupplierServiceBlocking;
        this.suppliersSupplierWorkflowServiceBlocking = suppliersSupplierWorkflowServiceBlocking;
    }

    /**
     * Starts a supplier contract.
     */
    public StartSupplierContractWorkflowResponse startSupplierContractWorkflow(StartSupplierContractWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartSupplierContractWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierContract(request.getContext().getTenantId(), request.getSupplierContract());

        var outbound0 = QuotePreferenceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(QuotePreferenceRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getSupplierContract().getBillingAddress().getLinesList())
                                .setPostalCode(request.getSupplierContract().getBillingAddress().getPostalCode())
                                .setRecipient(request.getSupplierContract().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> customersPreferenceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePreference(outbound0), rpcExecutor);

        var outbound1 = ResolveHouseholdsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> customersHouseholdQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveHouseholds(outbound1), rpcExecutor);

        var outbound2 = FindSupplierContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindSupplierContractsRequest.Filter.newBuilder()
                        .setRegion(request.getSupplierContract().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending2 = CompletableFuture.supplyAsync(() -> suppliersSupplierContractQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findSupplierContracts(outbound2), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1, pending2).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        var reply2 = pending2.join();
        log.atDebug().addKeyValue("auditValue", request.getSupplierContract().getSupplierContractId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartSupplierContractWorkflow", request.getSupplierContract().getSupplierContractId());
        return StartSupplierContractWorkflowResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setMessage(reply2.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setSupplierContract(SupplierContract.newBuilder()
                        .setDueAt(request.getSupplierContract().getDueAt())
                        .addLines(SupplierContractLine.newBuilder()
                                .setAmount(reply0.getPreference().getDiscount().getAmount())
                                .setDescription(reply0.getPreference().getDescription())
                                .build())
                        .setSupplierContractId(request.getSupplierContractId())
                        .build())
                .build();
    }

    /**
     * Advances a supplier contract.
     */
    public AdvanceSupplierContractWorkflowResponse advanceSupplierContractWorkflow(AdvanceSupplierContractWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceSupplierContractWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierContract(request.getContext().getTenantId(), request.getSupplierContract());

        var outbound0 = BeanCopies.copy(SimulateLineageNodeRequest.newBuilder()
                .setContext(request.getContext())
                .setLineageNode(LineageNode.newBuilder()
                        .addChildren(LineageNode.newBuilder()
                                .addChildren(LineageNode.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .setRegion(request.getSupplierContract().getBillingAddress().getRegion())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(request.getSupplierContract().getCounterparty().getDisplayName())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build(), SimulateLineageNodeRequest.class);
        var reply0 = lineageLineageNodeCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateLineageNode(outbound0);

        var outbound1 = CreateSupplierRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplier(Supplier.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getLineageNode().getAudit().getUpdatedBy())
                                .build())
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getSupplierContract().getBillingAddress().getLinesList())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setEmail(reply0.getLineageNode().getManager().getEmail())
                                .build())
                        .build())
                .build();
        RpcPort<CreateSupplierRequest, CreateSupplierResponse> port1 = RpcPort.adapt(suppliersSupplierServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::createSupplier);
        var reply1 = port1.exchange(outbound1);

        List<SearchAuditEventsResponse> results2 = request.getSupplierContract().getLinesList().stream()
                .map(element -> SearchAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchAuditEventsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getLineageNode().getAudit().getCreatedAt())
                        .putAllLabels(element.getAttributesMap())
                        .setOwnerId(element.getLineId())
                        .setRegion(reply0.getLineageNode().getLocation().getRegion())
                        .build())
                .build())
                .flatMap(value -> Stream.of(auditAuditEventServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchAuditEvents(value)))
                .collect(Collectors.toList());
        var reply2 = results2.isEmpty() ? SearchAuditEventsResponse.getDefaultInstance()
                : results2.getLast();

        var outbound3 = NormalizeSupplierRiskRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRisk(SupplierRisk.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply1.getSupplier().getContact().getEmail())
                                .build())
                        .build())
                .build();
        var reply3 = Optional.of(outbound3).map(value -> value.toBuilder().build())
                .map(value -> suppliersSupplierRiskValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeSupplierRisk(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getSupplierContract().getSupplierContractId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceSupplierContractWorkflow", request.getSupplierContract().getSupplierContractId());
        return AdvanceSupplierContractWorkflowResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply3.getResult().getCorrelationId())
                        .build())
                .setSupplierContract(SupplierContract.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getSupplier().getAudit().getUpdatedAt())
                                .setUpdatedBy(reply1.getSupplier().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getLineageNode().getLocation().getLinesList())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(reply3.getSupplierRisk().getAssignee().getEmail())
                                .build())
                        .setNumber(request.getSupplierContract().getNumber())
                        .setSupplierContractId(request.getSupplierContractId())
                        .build())
                .build();
    }

    /**
     * Cancels a supplier contract.
     */
    public CancelSupplierContractWorkflowResponse cancelSupplierContractWorkflow(CancelSupplierContractWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelSupplierContractWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierContract(request.getContext().getTenantId(), request.getSupplierContract());

        var outbound0 = ValidateAuditTrailRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getSupplierContract().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getSupplierContract().getBillingAddress().getRegion())
                                .build())
                        .setCard(AuditTrailCardSettlement.newBuilder()
                                .setLast4(switch (request.getSupplierContract().getSettlementCase()) {
                                    case CARD -> request.getSupplierContract().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ValidateAuditTrailRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = auditAuditTrailValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateAuditTrail(outbound0);

        var outbound1 = RetrySupplierWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplier(Supplier.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getAuditTrail().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        final class Stage1Adapter {
            RetrySupplierWorkflowResponse fetch(RetrySupplierWorkflowRequest input) {
                return suppliersSupplierWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retrySupplierWorkflow(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);

        var outbound2 = RecalculateConsentRecordsRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(HouseholdRef.newBuilder()
                                .setDisplayName(reply1.getSupplier().getRelationshipManager().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RecalculateConsentRecordsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getAuditTrail().getBillingAddress().getCountryCode())
                                .setPostalCode(reply1.getSupplier().getPrimaryAddress().getPostalCode())
                                .setRecipient(reply0.getAuditTrail().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.customers.v1.ConsentRecordCalculationService");
        var serviceName2 = "acme.customers.v1" + "." + "ConsentRecordCalculationService";
        var methodName2 = "Rec" + "alculateConsentRecords";
        var descriptor2 = MethodDescriptor.<RecalculateConsentRecordsRequest, RecalculateConsentRecordsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(RecalculateConsentRecordsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(RecalculateConsentRecordsResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);
        log.atDebug().addKeyValue("auditValue", request.getSupplierContract().getSupplierContractId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelSupplierContractWorkflow", request.getSupplierContract().getSupplierContractId());
        return CancelSupplierContractWorkflowResponse.newBuilder()
                .setOutcome(CancelSupplierContractWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setSupplierContract(SupplierContract.newBuilder()
                        .addLines(SupplierContractLine.newBuilder()
                                .putAllAttributes(reply2.getConsentRecord().getAttributesMap())
                                .build())
                        .setNotes(request.getSupplierContract().getNotes())
                        .setSupplierContractId(request.getSupplierContractId())
                        .build())
                .build();
    }

    /**
     * Retrys a supplier contract.
     */
    public RetrySupplierContractWorkflowResponse retrySupplierContractWorkflow(RetrySupplierContractWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetrySupplierContractWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierContract(request.getContext().getTenantId(), request.getSupplierContract());

        var outbound0 = DelegateSupplierRatingApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setBank(SupplierRatingBankSettlement.newBuilder()
                                .setAccountHolder(request.getSupplierContract().getBank().getAccountHolder())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getSupplierContract().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> suppliersSupplierRatingApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).delegateSupplierRatingApproval(outbound0), rpcExecutor);

        var outbound1 = NormalizeSupplierRatingRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setBank(SupplierRatingBankSettlement.newBuilder()
                                .setAccountHolder(request.getSupplierContract().getBank().getAccountHolder())
                                .setIban(request.getSupplierContract().getBank().getIban())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getSupplierContract().getBillingAddress().getLinesList())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getSupplierContract().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> suppliersSupplierRatingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeSupplierRating(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getSupplierContract().getSupplierContractId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetrySupplierContractWorkflow", request.getSupplierContract().getSupplierContractId());
        return RetrySupplierContractWorkflowResponse.newBuilder()
                .setOutcome(RetrySupplierContractWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .setSupplierContract(SupplierContract.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply1.getSupplierRating().getBillingAddress().getLocality())
                                .build())
                        .setStatus(request.getSupplierContract().getStatus())
                        .setSupplierContractId(request.getSupplierContractId())
                        .build())
                .build();
    }

    /**
     * Pauses a supplier contract.
     */
    public PauseSupplierContractWorkflowResponse pauseSupplierContractWorkflow(PauseSupplierContractWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseSupplierContractWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierContract(request.getContext().getTenantId(), request.getSupplierContract());

        var outbound0 = ResolveJournalEntriesRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        Function<ResolveJournalEntriesRequest, ResolveJournalEntriesResponse> operation0 = ledgerJournalEntryQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::resolveJournalEntries;
        var reply0 = operation0.apply(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getSupplierContract().getSupplierContractId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseSupplierContractWorkflow", request.getSupplierContract().getSupplierContractId());
        return PauseSupplierContractWorkflowResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setSupplierContract(SupplierContract.newBuilder()
                        .setBillingAddress(request.getSupplierContract().getBillingAddress())
                        .setSupplierContractId(request.getSupplierContractId())
                        .build())
                .build();
    }

    /**
     * Resumes a supplier contract.
     */
    public ResumeSupplierContractWorkflowResponse resumeSupplierContractWorkflow(ResumeSupplierContractWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeSupplierContractWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierContract(request.getContext().getTenantId(), request.getSupplierContract());

        var results0 = new ArrayList<ListCustomersResponse>();
        for (var element : request.getSupplierContract().getLinesList()) {
            ListCustomersRequest itemRequest = ListCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListCustomersRequest.Filter.newBuilder()
                        .setOwnerId(element.getLineId())
                        .setRegion(request.getSupplierContract().getBillingAddress().getRegion())
                        .build())
                .build();
            results0.add(customersCustomerServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listCustomers(itemRequest));
        }
        var reply0 = results0.isEmpty() ? ListCustomersResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = StreamRepaymentMetricsRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setPhone(request.getDetails().getNotify().getPhone())
                        .build())
                .build();
        var stream1 = lendingRepaymentReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamRepaymentMetrics(outbound1);
        var reply1 = StreamRepaymentMetricsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getSupplierContract().getSupplierContractId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeSupplierContractWorkflow", request.getSupplierContract().getSupplierContractId());
        return ResumeSupplierContractWorkflowResponse.newBuilder()
                .setSupplierContract(SupplierContract.newBuilder()
                        .setIssuedAt(request.getSupplierContract().getIssuedAt())
                        .setSupplierContractId(request.getSupplierContractId())
                        .build())
                .build();
    }

    /**
     * Returns a supplier contract.
     */
    public InspectSupplierContractWorkflowResponse inspectSupplierContractWorkflow(InspectSupplierContractWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectSupplierContractWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "SupplierContract", request.getSupplierContractId(), "InspectSupplierContractWorkflow");

        var outbound0 = (CalculatePreferenceRequest.newBuilder()
                .setContext(request.getContext())
                .setPreference(Preference.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = customersPreferenceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculatePreference(outbound0);

        var outbound1 = ResumeBudgetLineWorkflowRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getPreference().getAudit().getEtag())
                                .build())
                        .addLines(BudgetLineLine.newBuilder()
                                .setTax(BudgetLineLineTax.newBuilder()
                                        .setAmount(reply0.getPreference().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> budgetingBudgetLineWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resumeBudgetLineWorkflow(value));
        return InspectSupplierContractWorkflowResponse.newBuilder()
                .setSupplierContract(SupplierContract.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply1.getBudgetLine().getBillingAddress().getLinesList())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setDisplayName(reply1.getBudgetLine().getCounterparty().getDisplayName())
                                .setEmail(reply0.getPreference().getOwner().getEmail())
                                .build())
                        .setSupplierContractId(request.getSupplierContractId())
                        .build())
                .build();
    }

    /**
     * Lists supplier contracts a page at a time.
     */
    public ListSupplierContractWorkflowsResponse listSupplierContractWorkflows(ListSupplierContractWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListSupplierContractWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "SupplierContract", request.getContext().getTraceId(), "ListSupplierContractWorkflows");

        var outbound0 = mapper.listSupplierContractWorkflowsStage0(request);
        var reply0 = suppliersSupplierContractQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveSupplierContracts(outbound0);

        var outbound1 = CheckSupplierRatingEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckSupplierRatingEligibilityRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = suppliersSupplierRatingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkSupplierRatingEligibility(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListSupplierContractWorkflows", request.getPage().getPageToken());
        return ListSupplierContractWorkflowsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .addSupplierContracts(SupplierContract.newBuilder()
                        .setDueAt(reply1.getSupplierRating().getDueAt())
                        .build())
                .build();
    }

    /**
     * Signals a supplier contract.
     */
    public SignalSupplierContractWorkflowResponse signalSupplierContractWorkflow(SignalSupplierContractWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalSupplierContractWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierContract(request.getContext().getTenantId(), request.getSupplierContract());

        var outbound0 = CheckSupplierRiskEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRisk(SupplierRisk.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            CheckSupplierRiskEligibilityResponse fetch(CheckSupplierRiskEligibilityRequest input) {
                return suppliersSupplierRiskValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkSupplierRiskEligibility(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = ExportSupplierContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportSupplierContractsRequest.Filter.newBuilder()
                        .setRegion(request.getSupplierContract().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream1 = suppliersSupplierContractQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportSupplierContracts(outbound1);
        var reply1 = ExportSupplierContractsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getSupplierContract().getSupplierContractId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalSupplierContractWorkflow", request.getSupplierContract().getSupplierContractId());
        return SignalSupplierContractWorkflowResponse.newBuilder()
                .setOutcome(SignalSupplierContractWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setSupplierContract(SupplierContract.newBuilder()
                        .setStatus(request.getSupplierContract().getStatus())
                        .setSupplierContractId(request.getSupplierContractId())
                        .build())
                .build();
    }
}
