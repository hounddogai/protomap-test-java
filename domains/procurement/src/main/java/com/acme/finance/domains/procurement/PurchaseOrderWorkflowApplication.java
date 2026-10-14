package com.acme.finance.domains.procurement;

import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationParticipant;
import com.acme.assets.v1.DepreciationWorkflowServiceGrpc;
import com.acme.assets.v1.PauseDepreciationWorkflowRequest;
import com.acme.assets.v1.PauseDepreciationWorkflowResponse;
import com.acme.banking.v1.BankAccountServiceGrpc;
import com.acme.banking.v1.BatchCreateBankAccountsRequest;
import com.acme.banking.v1.BatchCreateBankAccountsResponse;
import com.acme.budgeting.v1.StreamVarianceChangesRequest;
import com.acme.budgeting.v1.StreamVarianceChangesResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceSyncServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.CommentObligationApprovalRequest;
import com.acme.contracts.v1.CommentObligationApprovalResponse;
import com.acme.contracts.v1.Obligation;
import com.acme.contracts.v1.ObligationApprovalServiceGrpc;
import com.acme.customers.v1.CalculateConsentRecordRequest;
import com.acme.customers.v1.CalculateConsentRecordResponse;
import com.acme.customers.v1.CalculateCustomerRequest;
import com.acme.customers.v1.CalculateCustomerResponse;
import com.acme.customers.v1.CheckHouseholdEligibilityRequest;
import com.acme.customers.v1.CheckHouseholdEligibilityResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordCalculationServiceGrpc;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.ScoreHouseholdRequest;
import com.acme.customers.v1.ScoreHouseholdResponse;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanCardFunding;
import com.acme.lending.v1.LoanWorkflowServiceGrpc;
import com.acme.lending.v1.PauseLoanWorkflowRequest;
import com.acme.lending.v1.PauseLoanWorkflowResponse;
import com.acme.procurement.v1.AdvancePurchaseOrderWorkflowRequest;
import com.acme.procurement.v1.AdvancePurchaseOrderWorkflowResponse;
import com.acme.procurement.v1.CancelPurchaseOrderWorkflowRequest;
import com.acme.procurement.v1.CancelPurchaseOrderWorkflowResponse;
import com.acme.procurement.v1.CommentPurchaseOrderApprovalRequest;
import com.acme.procurement.v1.CommentPurchaseOrderApprovalResponse;
import com.acme.procurement.v1.CommentPurchaseRequisitionApprovalRequest;
import com.acme.procurement.v1.CommentPurchaseRequisitionApprovalResponse;
import com.acme.procurement.v1.ExplainRfqDecisionRequest;
import com.acme.procurement.v1.ExplainRfqDecisionResponse;
import com.acme.procurement.v1.GoodsReceiptApprovalServiceGrpc;
import com.acme.procurement.v1.InspectPurchaseOrderWorkflowRequest;
import com.acme.procurement.v1.InspectPurchaseOrderWorkflowResponse;
import com.acme.procurement.v1.ListPurchaseOrderWorkflowsRequest;
import com.acme.procurement.v1.ListPurchaseOrderWorkflowsResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderApprovalServiceGrpc;
import com.acme.procurement.v1.PurchaseOrderAttachment;
import com.acme.procurement.v1.PurchaseOrderLine;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionApprovalServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionRef;
import com.acme.procurement.v1.RecallGoodsReceiptRequest;
import com.acme.procurement.v1.RecallGoodsReceiptResponse;
import com.acme.procurement.v1.RetryPurchaseOrderWorkflowRequest;
import com.acme.procurement.v1.RetryPurchaseOrderWorkflowResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqReportingServiceGrpc;
import com.acme.procurement.v1.RfqValidationServiceGrpc;
import com.acme.procurement.v1.SignalPurchaseOrderWorkflowRequest;
import com.acme.procurement.v1.SignalPurchaseOrderWorkflowResponse;
import com.acme.procurement.v1.StartPurchaseOrderWorkflowRequest;
import com.acme.procurement.v1.StartPurchaseOrderWorkflowResponse;
import com.acme.procurement.v1.StreamRfqMetricsRequest;
import com.acme.procurement.v1.StreamRfqMetricsResponse;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Runs the long-lived workflow that takes a purchase order from start to finish. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class PurchaseOrderWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(PurchaseOrderWorkflowApplication.class);

    private final DepreciationWorkflowServiceGrpc.DepreciationWorkflowServiceBlockingStub assetsDepreciationWorkflowServiceBlocking;

    private final BankAccountServiceGrpc.BankAccountServiceStub bankingBankAccountServiceAsync;

    private final VarianceSyncServiceGrpc.VarianceSyncServiceStub budgetingVarianceSyncServiceAsync;

    private final ObligationApprovalServiceGrpc.ObligationApprovalServiceBlockingStub contractsObligationApprovalServiceBlocking;

    private final ConsentRecordCalculationServiceGrpc.ConsentRecordCalculationServiceBlockingStub customersConsentRecordCalculationServiceBlocking;

    private final CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking;

    private final HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final LoanWorkflowServiceGrpc.LoanWorkflowServiceBlockingStub lendingLoanWorkflowServiceBlocking;

    private final GoodsReceiptApprovalServiceGrpc.GoodsReceiptApprovalServiceBlockingStub procurementGoodsReceiptApprovalServiceBlocking;

    private final PurchaseOrderApprovalServiceGrpc.PurchaseOrderApprovalServiceBlockingStub procurementPurchaseOrderApprovalServiceBlocking;

    private final PurchaseRequisitionApprovalServiceGrpc.PurchaseRequisitionApprovalServiceBlockingStub procurementPurchaseRequisitionApprovalServiceBlocking;

    private final RfqReportingServiceGrpc.RfqReportingServiceBlockingStub procurementRfqReportingServiceBlocking;

    private final RfqValidationServiceGrpc.RfqValidationServiceBlockingStub procurementRfqValidationServiceBlocking;

    private final ProcurementRepository repository;

    private final Executor rpcExecutor;

    public PurchaseOrderWorkflowApplication(
            DepreciationWorkflowServiceGrpc.DepreciationWorkflowServiceBlockingStub assetsDepreciationWorkflowServiceBlocking,
            BankAccountServiceGrpc.BankAccountServiceStub bankingBankAccountServiceAsync,
            VarianceSyncServiceGrpc.VarianceSyncServiceStub budgetingVarianceSyncServiceAsync,
            ObligationApprovalServiceGrpc.ObligationApprovalServiceBlockingStub contractsObligationApprovalServiceBlocking,
            ConsentRecordCalculationServiceGrpc.ConsentRecordCalculationServiceBlockingStub customersConsentRecordCalculationServiceBlocking,
            CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking,
            HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking,
            ApplicationEventPublisher events,
            LoanWorkflowServiceGrpc.LoanWorkflowServiceBlockingStub lendingLoanWorkflowServiceBlocking,
            GoodsReceiptApprovalServiceGrpc.GoodsReceiptApprovalServiceBlockingStub procurementGoodsReceiptApprovalServiceBlocking,
            PurchaseOrderApprovalServiceGrpc.PurchaseOrderApprovalServiceBlockingStub procurementPurchaseOrderApprovalServiceBlocking,
            PurchaseRequisitionApprovalServiceGrpc.PurchaseRequisitionApprovalServiceBlockingStub procurementPurchaseRequisitionApprovalServiceBlocking,
            RfqReportingServiceGrpc.RfqReportingServiceBlockingStub procurementRfqReportingServiceBlocking,
            RfqValidationServiceGrpc.RfqValidationServiceBlockingStub procurementRfqValidationServiceBlocking,
            ProcurementRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.assetsDepreciationWorkflowServiceBlocking = assetsDepreciationWorkflowServiceBlocking;
        this.bankingBankAccountServiceAsync = bankingBankAccountServiceAsync;
        this.budgetingVarianceSyncServiceAsync = budgetingVarianceSyncServiceAsync;
        this.contractsObligationApprovalServiceBlocking = contractsObligationApprovalServiceBlocking;
        this.customersConsentRecordCalculationServiceBlocking = customersConsentRecordCalculationServiceBlocking;
        this.customersCustomerCalculationServiceBlocking = customersCustomerCalculationServiceBlocking;
        this.customersHouseholdValidationServiceBlocking = customersHouseholdValidationServiceBlocking;
        this.events = events;
        this.lendingLoanWorkflowServiceBlocking = lendingLoanWorkflowServiceBlocking;
        this.procurementGoodsReceiptApprovalServiceBlocking = procurementGoodsReceiptApprovalServiceBlocking;
        this.procurementPurchaseOrderApprovalServiceBlocking = procurementPurchaseOrderApprovalServiceBlocking;
        this.procurementPurchaseRequisitionApprovalServiceBlocking = procurementPurchaseRequisitionApprovalServiceBlocking;
        this.procurementRfqReportingServiceBlocking = procurementRfqReportingServiceBlocking;
        this.procurementRfqValidationServiceBlocking = procurementRfqValidationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    public record StartPurchaseOrderWorkflowStage0Requested(PauseDepreciationWorkflowRequest request, CompletableFuture<PauseDepreciationWorkflowResponse> completion, io.grpc.Context context) {}

    /**
     * Starts a purchase order.
     */
    public StartPurchaseOrderWorkflowResponse startPurchaseOrderWorkflow(StartPurchaseOrderWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartPurchaseOrderWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseOrder(request.getContext().getTenantId(), request.getPurchaseOrder());

        var outbound0 = PauseDepreciationWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDepreciation(Depreciation.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setCountryCode(request.getPurchaseOrder().getBillingAddress().getCountryCode())
                                .build())
                        .addParticipants(DepreciationParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setName(request.getDetails().getNotify().getName())
                                        .build())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<PauseDepreciationWorkflowResponse>();
        events.publishEvent(new StartPurchaseOrderWorkflowStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = CalculateConsentRecordRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(HouseholdRef.newBuilder()
                                .setDisplayName(reply0.getDepreciation().getOrganizer().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CalculateConsentRecordRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getDepreciation().getLocation().getCountryCode())
                                .addAllLines(request.getPurchaseOrder().getBillingAddress().getLinesList())
                                .setRegion(request.getPurchaseOrder().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        Function<CalculateConsentRecordRequest, CalculateConsentRecordResponse> operation1 = customersConsentRecordCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::calculateConsentRecord;
        var reply1 = operation1.apply(outbound1);

        var outbound2 = BatchCreateBankAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateBankAccountsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getNotify())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(BatchCreateBankAccountsResponse.getDefaultInstance());
        var input2 = bankingBankAccountServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateBankAccounts(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = StreamVarianceChangesRequest.newBuilder()
                .setChange(Variance.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getPurchaseOrder().getCounterparty().getDisplayName())
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getPurchaseOrder().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamVarianceChangesResponse.getDefaultInstance());
        var input3 = budgetingVarianceSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamVarianceChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getPurchaseOrder().getPurchaseOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartPurchaseOrderWorkflow", request.getPurchaseOrder().getPurchaseOrderId());
        return StartPurchaseOrderWorkflowResponse.newBuilder()
                .setOutcome(StartPurchaseOrderWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setBillingAddress(request.getPurchaseOrder().getBillingAddress())
                        .setCounterparty(PurchaseRequisitionRef.newBuilder()
                                .setDisplayName(reply0.getDepreciation().getOrganizer().getDisplayName())
                                .build())
                        .addLines(PurchaseOrderLine.newBuilder()
                                .putAllAttributes(reply1.getConsentRecord().getAttributesMap())
                                .build())
                        .setPurchaseOrderId(request.getPurchaseOrderId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Advances a purchase order.
     */
    public AdvancePurchaseOrderWorkflowResponse advancePurchaseOrderWorkflow(AdvancePurchaseOrderWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvancePurchaseOrderWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseOrder(request.getContext().getTenantId(), request.getPurchaseOrder());

        var outbound0 = CommentObligationApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setObligation(Obligation.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            CommentObligationApprovalResponse fetch(CommentObligationApprovalRequest input) {
                return contractsObligationApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentObligationApproval(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = CalculateCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .build())
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setRegion(request.getPurchaseOrder().getBillingAddress().getRegion())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setEmail(reply0.getObligation().getCustomer().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = customersCustomerCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateCustomer(outbound1);

        var outbound2 = RecallGoodsReceiptRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecallGoodsReceiptRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(reply1.getCustomer().getContact().getEmail())
                                .build())
                        .build())
                .build();
        var reply2 = procurementGoodsReceiptApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallGoodsReceipt(outbound2);

        var results3 = new ArrayList<CommentPurchaseOrderApprovalResponse>();
        for (var element : request.getPurchaseOrder().getAttachmentsList()) {
            CommentPurchaseOrderApprovalRequest itemRequest = CommentPurchaseOrderApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .addAttachments(PurchaseOrderAttachment.newBuilder()
                                .setChecksum(element.getChecksum())
                                .setContentType(element.getContentType())
                                .setSizeBytes(element.getSizeBytes())
                                .build())
                        .build())
                .build();
            results3.add(procurementPurchaseOrderApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentPurchaseOrderApproval(itemRequest));
        }
        var reply3 = results3.isEmpty() ? CommentPurchaseOrderApprovalResponse.getDefaultInstance()
                : results3.getLast();
        log.atDebug().addKeyValue("auditValue", request.getPurchaseOrder().getPurchaseOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvancePurchaseOrderWorkflow", request.getPurchaseOrder().getPurchaseOrderId());
        return AdvancePurchaseOrderWorkflowResponse.newBuilder()
                .setOutcome(AdvancePurchaseOrderWorkflowResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply2.getOutcome().getEffectiveAt())
                        .build())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setCounterparty(PurchaseRequisitionRef.newBuilder()
                                .setNumber(reply1.getCustomer().getNationalId().getNumber())
                                .build())
                        .setDueAt(reply2.getGoodsReceipt().getDueAt())
                        .putAllMetadata(request.getPurchaseOrder().getMetadataMap())
                        .setPurchaseOrderId(request.getPurchaseOrderId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Cancels a purchase order.
     */
    public CancelPurchaseOrderWorkflowResponse cancelPurchaseOrderWorkflow(CancelPurchaseOrderWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelPurchaseOrderWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseOrder(request.getContext().getTenantId(), request.getPurchaseOrder());

        var outbound0 = StreamRfqMetricsRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                        .build())
                .build();
        var stream0 = procurementRfqReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamRfqMetrics(outbound0);
        var reply0 = StreamRfqMetricsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = PauseDepreciationWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDepreciation(Depreciation.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setLocality(request.getPurchaseOrder().getBillingAddress().getLocality())
                                .setRegion(request.getPurchaseOrder().getBillingAddress().getRegion())
                                .build())
                        .addParticipants(DepreciationParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setName(request.getDetails().getNotify().getName())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> assetsDepreciationWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pauseDepreciationWorkflow(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getPurchaseOrder().getPurchaseOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelPurchaseOrderWorkflow", request.getPurchaseOrder().getPurchaseOrderId());
        return CancelPurchaseOrderWorkflowResponse.newBuilder()
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getDepreciation().getAudit().getUpdatedBy())
                                .build())
                        .setNotes(request.getPurchaseOrder().getNotes())
                        .setPurchaseOrderId(request.getPurchaseOrderId())
                        .build())
                .build();
    }

    /**
     * Retrys a purchase order.
     */
    public RetryPurchaseOrderWorkflowResponse retryPurchaseOrderWorkflow(RetryPurchaseOrderWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryPurchaseOrderWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseOrder(request.getContext().getTenantId(), request.getPurchaseOrder());

        var outbound0 = ScoreHouseholdRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .setPostalCode(request.getPurchaseOrder().getBillingAddress().getPostalCode())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        RpcPort<ScoreHouseholdRequest, ScoreHouseholdResponse> port0 = RpcPort.adapt(customersHouseholdValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::scoreHousehold);
        var reply0 = port0.exchange(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPurchaseOrder().getPurchaseOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryPurchaseOrderWorkflow", request.getPurchaseOrder().getPurchaseOrderId());
        return RetryPurchaseOrderWorkflowResponse.newBuilder()
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setCounterparty(PurchaseRequisitionRef.newBuilder()
                                .setDisplayName(reply0.getHousehold().getManager().getDisplayName())
                                .build())
                        .putAllMetadata(request.getPurchaseOrder().getMetadataMap())
                        .setPurchaseOrderId(request.getPurchaseOrderId())
                        .build())
                .build();
    }

    /**
     * Returns a purchase order.
     */
    public InspectPurchaseOrderWorkflowResponse inspectPurchaseOrderWorkflow(InspectPurchaseOrderWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectPurchaseOrderWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseOrder", request.getPurchaseOrderId(), "InspectPurchaseOrderWorkflow");

        var outbound0 = ExplainRfqDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainRfqDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setRfq(Rfq.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        Function<ExplainRfqDecisionRequest, ExplainRfqDecisionResponse> operation0 = procurementRfqValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::explainRfqDecision;
        var reply0 = operation0.apply(outbound0);
        return InspectPurchaseOrderWorkflowResponse.newBuilder()
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getRfq().getAudit().getUpdatedBy())
                                .build())
                        .setPurchaseOrderId(request.getPurchaseOrderId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Lists purchase orders a page at a time.
     */
    public ListPurchaseOrderWorkflowsResponse listPurchaseOrderWorkflows(ListPurchaseOrderWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPurchaseOrderWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseOrder", request.getContext().getTraceId(), "ListPurchaseOrderWorkflows");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPurchaseOrderWorkflows", request.getPage().getPageToken());
        return ListPurchaseOrderWorkflowsResponse.getDefaultInstance();
    }

    /**
     * Signals a purchase order.
     */
    public SignalPurchaseOrderWorkflowResponse signalPurchaseOrderWorkflow(SignalPurchaseOrderWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalPurchaseOrderWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseOrder(request.getContext().getTenantId(), request.getPurchaseOrder());

        var outbound0 = PauseLoanWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getPurchaseOrder().getAudit().getCreatedAt())
                                .build())
                        .setCard(LoanCardFunding.newBuilder()
                                .setToken(switch (request.getPurchaseOrder().getSettlementCase()) {
                                    case CARD -> request.getPurchaseOrder().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = lendingLoanWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pauseLoanWorkflow(outbound0);

        var outbound1 = CommentPurchaseRequisitionApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CommentPurchaseRequisitionApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getPurchaseOrder().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getLoan().getAudit().getCreatedAt())
                                .setCreatedBy(reply0.getLoan().getAudit().getCreatedBy())
                                .setUpdatedBy(reply0.getLoan().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        RpcPort<CommentPurchaseRequisitionApprovalRequest, CommentPurchaseRequisitionApprovalResponse> port1 = RpcPort.adapt(procurementPurchaseRequisitionApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::commentPurchaseRequisitionApproval);
        var reply1 = port1.exchange(outbound1);

        var futures2 = request.getPurchaseOrder().getLinesList().stream()
                .map(element -> CheckHouseholdEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(request.getPurchaseOrder().getAudit().getCreatedAt())
                                                .build())
                                        .addChildren(Household.newBuilder()
                                                .putAllAttributes(element.getAttributesMap())
                                                .build())
                                        .setLocation(PostalAddress.newBuilder()
                                                .setLocality(request.getPurchaseOrder().getBillingAddress().getLocality())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(reply0.getLoan().getHolder().getDisplayName())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> customersHouseholdValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkHouseholdEligibility(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures2.toArray(CompletableFuture[]::new)).join();
        var results2 = futures2.stream().map(CompletableFuture::join).toList();
        var reply2 = results2.isEmpty() ? CheckHouseholdEligibilityResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getPurchaseOrder().getPurchaseOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalPurchaseOrderWorkflow", request.getPurchaseOrder().getPurchaseOrderId());
        return SignalPurchaseOrderWorkflowResponse.newBuilder()
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getLoan().getAudit().getEtag())
                                .build())
                        .setDueAt(request.getPurchaseOrder().getDueAt())
                        .setPurchaseOrderId(request.getPurchaseOrderId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
