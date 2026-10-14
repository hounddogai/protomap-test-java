package com.acme.finance.domains.procurement;

import com.acme.appointments.v1.Appointment;
import com.acme.appointments.v1.AppointmentParticipant;
import com.acme.appointments.v1.AppointmentServiceGrpc;
import com.acme.appointments.v1.CreateAppointmentRequest;
import com.acme.appointments.v1.CreateAppointmentResponse;
import com.acme.assets.v1.ApproveAssetTransferRequest;
import com.acme.assets.v1.ApproveAssetTransferResponse;
import com.acme.assets.v1.AssetTransferApprovalServiceGrpc;
import com.acme.assets.v1.DepreciationWorkflowServiceGrpc;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalSyncServiceGrpc;
import com.acme.assets.v1.ListDepreciationWorkflowsRequest;
import com.acme.assets.v1.ListDepreciationWorkflowsResponse;
import com.acme.assets.v1.StreamDisposalChangesRequest;
import com.acme.assets.v1.StreamDisposalChangesResponse;
import com.acme.banking.v1.BankAccount;
import com.acme.banking.v1.BankAccountWorkflowServiceGrpc;
import com.acme.banking.v1.CancelBankAccountWorkflowRequest;
import com.acme.banking.v1.CancelBankAccountWorkflowResponse;
import com.acme.budgeting.v1.BudgetWorkflowServiceGrpc;
import com.acme.budgeting.v1.ImportVariancesRequest;
import com.acme.budgeting.v1.ImportVariancesResponse;
import com.acme.budgeting.v1.ListBudgetWorkflowsRequest;
import com.acme.budgeting.v1.ListBudgetWorkflowsResponse;
import com.acme.budgeting.v1.VarianceImportServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CompareCustomerSegmentOptionsRequest;
import com.acme.customers.v1.CompareCustomerSegmentOptionsResponse;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.ScoreCustomerRequest;
import com.acme.customers.v1.ScoreCustomerResponse;
import com.acme.customers.v1.ValidateHouseholdRequest;
import com.acme.customers.v1.ValidateHouseholdResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.procurement.v1.AdvancePurchaseRequisitionWorkflowRequest;
import com.acme.procurement.v1.AdvancePurchaseRequisitionWorkflowResponse;
import com.acme.procurement.v1.CancelPurchaseRequisitionWorkflowRequest;
import com.acme.procurement.v1.CancelPurchaseRequisitionWorkflowResponse;
import com.acme.procurement.v1.InspectPurchaseRequisitionWorkflowRequest;
import com.acme.procurement.v1.InspectPurchaseRequisitionWorkflowResponse;
import com.acme.procurement.v1.ListPendingPurchaseOrdersRequest;
import com.acme.procurement.v1.ListPendingPurchaseOrdersResponse;
import com.acme.procurement.v1.ListPurchaseRequisitionWorkflowsRequest;
import com.acme.procurement.v1.ListPurchaseRequisitionWorkflowsResponse;
import com.acme.procurement.v1.PurchaseOrderApprovalServiceGrpc;
import com.acme.procurement.v1.PurchaseOrderQueryServiceGrpc;
import com.acme.procurement.v1.PurchaseOrderRef;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionLine;
import com.acme.procurement.v1.PurchaseRequisitionServiceGrpc;
import com.acme.procurement.v1.ResumePurchaseRequisitionWorkflowRequest;
import com.acme.procurement.v1.ResumePurchaseRequisitionWorkflowResponse;
import com.acme.procurement.v1.RetryPurchaseRequisitionWorkflowRequest;
import com.acme.procurement.v1.RetryPurchaseRequisitionWorkflowResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqLifecycleServiceGrpc;
import com.acme.procurement.v1.RfqSla;
import com.acme.procurement.v1.ScheduleRfqExpiryRequest;
import com.acme.procurement.v1.ScheduleRfqExpiryResponse;
import com.acme.procurement.v1.SearchPurchaseRequisitionsRequest;
import com.acme.procurement.v1.SearchPurchaseRequisitionsResponse;
import com.acme.procurement.v1.SignalPurchaseRequisitionWorkflowRequest;
import com.acme.procurement.v1.SignalPurchaseRequisitionWorkflowResponse;
import com.acme.procurement.v1.StartPurchaseRequisitionWorkflowRequest;
import com.acme.procurement.v1.StartPurchaseRequisitionWorkflowResponse;
import com.acme.procurement.v1.SuggestPurchaseOrdersRequest;
import com.acme.procurement.v1.SuggestPurchaseOrdersResponse;
import com.acme.venues.v1.ValidateVenueRequest;
import com.acme.venues.v1.ValidateVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueComponent;
import com.acme.venues.v1.VenueValidationServiceGrpc;
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
import org.springframework.stereotype.Service;

/**
 * Runs the long-lived workflow that takes a purchase requisition from start to finish. Coordinates validation,
 * durable audit writes, and downstream operations.
 */
@Service
public class PurchaseRequisitionWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(PurchaseRequisitionWorkflowApplication.class);

    private final AssetTransferApprovalServiceGrpc.AssetTransferApprovalServiceBlockingStub assetsAssetTransferApprovalServiceBlocking;

    private final DepreciationWorkflowServiceGrpc.DepreciationWorkflowServiceBlockingStub assetsDepreciationWorkflowServiceBlocking;

    private final DisposalSyncServiceGrpc.DisposalSyncServiceStub assetsDisposalSyncServiceAsync;

    private final BankAccountWorkflowServiceGrpc.BankAccountWorkflowServiceBlockingStub bankingBankAccountWorkflowServiceBlocking;

    private final BudgetWorkflowServiceGrpc.BudgetWorkflowServiceBlockingStub budgetingBudgetWorkflowServiceBlocking;

    private final VarianceImportServiceGrpc.VarianceImportServiceStub budgetingVarianceImportServiceAsync;

    private final ChannelRegistry channels;

    private final CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking;

    private final CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking;

    private final HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking;

    private final PurchaseRequisitionWorkflowRequestMapper mapper;

    private final PurchaseOrderApprovalServiceGrpc.PurchaseOrderApprovalServiceBlockingStub procurementPurchaseOrderApprovalServiceBlocking;

    private final PurchaseOrderQueryServiceGrpc.PurchaseOrderQueryServiceBlockingStub procurementPurchaseOrderQueryServiceBlocking;

    private final PurchaseRequisitionServiceGrpc.PurchaseRequisitionServiceBlockingStub procurementPurchaseRequisitionServiceBlocking;

    private final RfqLifecycleServiceGrpc.RfqLifecycleServiceBlockingStub procurementRfqLifecycleServiceBlocking;

    private final ProcurementRepository repository;

    private final Executor rpcExecutor;

    private final VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking;

    public PurchaseRequisitionWorkflowApplication(
            AssetTransferApprovalServiceGrpc.AssetTransferApprovalServiceBlockingStub assetsAssetTransferApprovalServiceBlocking,
            DepreciationWorkflowServiceGrpc.DepreciationWorkflowServiceBlockingStub assetsDepreciationWorkflowServiceBlocking,
            DisposalSyncServiceGrpc.DisposalSyncServiceStub assetsDisposalSyncServiceAsync,
            BankAccountWorkflowServiceGrpc.BankAccountWorkflowServiceBlockingStub bankingBankAccountWorkflowServiceBlocking,
            BudgetWorkflowServiceGrpc.BudgetWorkflowServiceBlockingStub budgetingBudgetWorkflowServiceBlocking,
            VarianceImportServiceGrpc.VarianceImportServiceStub budgetingVarianceImportServiceAsync,
            ChannelRegistry channels,
            CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking,
            CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking,
            HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking,
            PurchaseRequisitionWorkflowRequestMapper mapper,
            PurchaseOrderApprovalServiceGrpc.PurchaseOrderApprovalServiceBlockingStub procurementPurchaseOrderApprovalServiceBlocking,
            PurchaseOrderQueryServiceGrpc.PurchaseOrderQueryServiceBlockingStub procurementPurchaseOrderQueryServiceBlocking,
            PurchaseRequisitionServiceGrpc.PurchaseRequisitionServiceBlockingStub procurementPurchaseRequisitionServiceBlocking,
            RfqLifecycleServiceGrpc.RfqLifecycleServiceBlockingStub procurementRfqLifecycleServiceBlocking,
            ProcurementRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking) {
        this.assetsAssetTransferApprovalServiceBlocking = assetsAssetTransferApprovalServiceBlocking;
        this.assetsDepreciationWorkflowServiceBlocking = assetsDepreciationWorkflowServiceBlocking;
        this.assetsDisposalSyncServiceAsync = assetsDisposalSyncServiceAsync;
        this.bankingBankAccountWorkflowServiceBlocking = bankingBankAccountWorkflowServiceBlocking;
        this.budgetingBudgetWorkflowServiceBlocking = budgetingBudgetWorkflowServiceBlocking;
        this.budgetingVarianceImportServiceAsync = budgetingVarianceImportServiceAsync;
        this.channels = channels;
        this.customersCustomerSegmentCalculationServiceBlocking = customersCustomerSegmentCalculationServiceBlocking;
        this.customersCustomerValidationServiceBlocking = customersCustomerValidationServiceBlocking;
        this.customersHouseholdValidationServiceBlocking = customersHouseholdValidationServiceBlocking;
        this.mapper = mapper;
        this.procurementPurchaseOrderApprovalServiceBlocking = procurementPurchaseOrderApprovalServiceBlocking;
        this.procurementPurchaseOrderQueryServiceBlocking = procurementPurchaseOrderQueryServiceBlocking;
        this.procurementPurchaseRequisitionServiceBlocking = procurementPurchaseRequisitionServiceBlocking;
        this.procurementRfqLifecycleServiceBlocking = procurementRfqLifecycleServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.venuesVenueValidationServiceBlocking = venuesVenueValidationServiceBlocking;
    }

    /**
     * Starts a purchase requisition.
     */
    public StartPurchaseRequisitionWorkflowResponse startPurchaseRequisitionWorkflow(StartPurchaseRequisitionWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartPurchaseRequisitionWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseRequisition(request.getContext().getTenantId(), request.getPurchaseRequisition());

        var outbound0 = ScheduleRfqExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setRfq(Rfq.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getPurchaseRequisition().getAudit().getEtag())
                                .setUpdatedBy(request.getPurchaseRequisition().getAudit().getUpdatedBy())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .build())
                        .setSla(RfqSla.newBuilder()
                                .setDueAt(request.getPurchaseRequisition().getDueAt())
                                .build())
                        .build())
                .build();
        var reply0 = procurementRfqLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleRfqExpiry(outbound0);

        var outbound1 = JsonCodec.roundTrip(ListBudgetWorkflowsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListBudgetWorkflowsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getRfq().getAudit().getCreatedAt())
                        .putAllLabels(reply0.getRfq().getCustomFieldsMap())
                        .setRegion(request.getPurchaseRequisition().getBillingAddress().getRegion())
                        .build())
                .build(), ListBudgetWorkflowsRequest.class);
        var reply1 = budgetingBudgetWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listBudgetWorkflows(outbound1);

        var outbound2 = CompareCustomerSegmentOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getPurchaseRequisition().getAudit().getEtag())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(switch (reply0.getRfq().getOriginCase()) {
                                    case CUSTOMER -> reply0.getRfq().getCustomer().getDisplayName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply2 = customersCustomerSegmentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareCustomerSegmentOptions(outbound2);

        var outbound3 = ImportVariancesRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(ImportVariancesRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getNotify())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(ImportVariancesResponse.getDefaultInstance());
        var input3 = budgetingVarianceImportServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).importVariances(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();

        var outbound4 = StreamDisposalChangesRequest.newBuilder()
                .setChange(Disposal.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getPurchaseRequisition().getCounterparty().getDisplayName())
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getPurchaseRequisition().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(StreamDisposalChangesResponse.getDefaultInstance());
        var input4 = assetsDisposalSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamDisposalChanges(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();
        log.atDebug().addKeyValue("auditValue", request.getPurchaseRequisition().getPurchaseRequisitionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartPurchaseRequisitionWorkflow", request.getPurchaseRequisition().getPurchaseRequisitionId());
        return StartPurchaseRequisitionWorkflowResponse.newBuilder()
                .setOutcome(StartPurchaseRequisitionWorkflowResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply2.getCustomerSegment().getAudit().getCreatedBy())
                                .build())
                        .setCounterparty(PurchaseOrderRef.newBuilder()
                                .setNumber(request.getPurchaseRequisition().getNumber())
                                .build())
                        .setPurchaseRequisitionId(request.getPurchaseRequisitionId())
                        .setTotal(reply2.getBreakdown().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Advances a purchase requisition.
     */
    public AdvancePurchaseRequisitionWorkflowResponse advancePurchaseRequisitionWorkflow(AdvancePurchaseRequisitionWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvancePurchaseRequisitionWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseRequisition(request.getContext().getTenantId(), request.getPurchaseRequisition());

        var outbound0 = ValidateHouseholdRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setUpdatedAt(request.getPurchaseRequisition().getAudit().getUpdatedAt())
                                                .build())
                                        .setLocation(PostalAddress.newBuilder()
                                                .setCountryCode(request.getPurchaseRequisition().getBillingAddress().getCountryCode())
                                                .setLocality(request.getPurchaseRequisition().getBillingAddress().getLocality())
                                                .setPostalCode(request.getPurchaseRequisition().getBillingAddress().getPostalCode())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        RpcPort<ValidateHouseholdRequest, ValidateHouseholdResponse> port0 = RpcPort.adapt(customersHouseholdValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::validateHousehold);
        var reply0 = port0.exchange(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPurchaseRequisition().getPurchaseRequisitionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvancePurchaseRequisitionWorkflow", request.getPurchaseRequisition().getPurchaseRequisitionId());
        return AdvancePurchaseRequisitionWorkflowResponse.newBuilder()
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply0.getHousehold().getLocation().getRecipient())
                                .build())
                        .addLines(PurchaseRequisitionLine.newBuilder()
                                .putAllAttributes(reply0.getHousehold().getAttributesMap())
                                .build())
                        .setNotes(request.getPurchaseRequisition().getNotes())
                        .setPurchaseRequisitionId(request.getPurchaseRequisitionId())
                        .build())
                .build();
    }

    /**
     * Cancels a purchase requisition.
     */
    public CancelPurchaseRequisitionWorkflowResponse cancelPurchaseRequisitionWorkflow(CancelPurchaseRequisitionWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelPurchaseRequisitionWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseRequisition(request.getContext().getTenantId(), request.getPurchaseRequisition());

        var results0 = new ArrayList<ValidateVenueResponse>();
        for (var element : request.getPurchaseRequisition().getLinesList()) {
            ValidateVenueRequest itemRequest = ValidateVenueRequest.newBuilder()
                .setContext(request.getContext())
                .setVenue(Venue.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getPurchaseRequisition().getAudit().getUpdatedAt())
                                .build())
                        .putComponents("primary", VenueComponent.newBuilder()
                                .setDescription(element.getDescription())
                                .setQuantity(element.getQuantity())
                                .build())
                        .build())
                .build();
            results0.add(venuesVenueValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateVenue(itemRequest));
        }
        var reply0 = results0.isEmpty() ? ValidateVenueResponse.getDefaultInstance()
                : results0.getLast();
        log.atDebug().addKeyValue("auditValue", request.getPurchaseRequisition().getPurchaseRequisitionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelPurchaseRequisitionWorkflow", request.getPurchaseRequisition().getPurchaseRequisitionId());
        return CancelPurchaseRequisitionWorkflowResponse.newBuilder()
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setIssuedAt(request.getPurchaseRequisition().getIssuedAt())
                        .setPurchaseRequisitionId(request.getPurchaseRequisitionId())
                        .build())
                .build();
    }

    /**
     * Retrys a purchase requisition.
     */
    public RetryPurchaseRequisitionWorkflowResponse retryPurchaseRequisitionWorkflow(RetryPurchaseRequisitionWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryPurchaseRequisitionWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseRequisition(request.getContext().getTenantId(), request.getPurchaseRequisition());

        var outbound0 = SuggestPurchaseOrdersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestPurchaseOrdersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getPurchaseRequisition().getDueAt())
                        .putAllLabels(request.getPurchaseRequisition().getMetadataMap())
                        .setOwnerId(request.getPurchaseRequisition().getCounterparty().getPurchaseOrderId())
                        .setRegion(request.getPurchaseRequisition().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> procurementPurchaseOrderQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestPurchaseOrders(outbound0), rpcExecutor);

        var outbound1 = ListDepreciationWorkflowsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListDepreciationWorkflowsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getPurchaseRequisition().getAudit().getUpdatedAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getPurchaseRequisition().getCounterparty().getPurchaseOrderId())
                        .setRegion(request.getPurchaseRequisition().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> assetsDepreciationWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listDepreciationWorkflows(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getPurchaseRequisition().getPurchaseRequisitionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryPurchaseRequisitionWorkflow", request.getPurchaseRequisition().getPurchaseRequisitionId());
        return RetryPurchaseRequisitionWorkflowResponse.newBuilder()
                .setOutcome(RetryPurchaseRequisitionWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setPurchaseRequisitionId(request.getPurchaseRequisitionId())
                        .setTotal(request.getPurchaseRequisition().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Resumes a purchase requisition.
     */
    public ResumePurchaseRequisitionWorkflowResponse resumePurchaseRequisitionWorkflow(ResumePurchaseRequisitionWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumePurchaseRequisitionWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseRequisition(request.getContext().getTenantId(), request.getPurchaseRequisition());

        var outbound0 = mapper.resumePurchaseRequisitionWorkflowStage0(request);
        var reply0 = procurementPurchaseRequisitionServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchPurchaseRequisitions(outbound0);

        var outbound1 = ListPendingPurchaseOrdersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingPurchaseOrdersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getPurchaseRequisition().getAudit().getCreatedAt())
                        .setRegion(request.getPurchaseRequisition().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply1 = RpcSupport.retryRead(() -> procurementPurchaseOrderApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingPurchaseOrders(outbound1));
        log.atDebug().addKeyValue("auditValue", request.getPurchaseRequisition().getPurchaseRequisitionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumePurchaseRequisitionWorkflow", request.getPurchaseRequisition().getPurchaseRequisitionId());
        return ResumePurchaseRequisitionWorkflowResponse.newBuilder()
                .setOutcome(ResumePurchaseRequisitionWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setBillingAddress(request.getPurchaseRequisition().getBillingAddress())
                        .setPurchaseRequisitionId(request.getPurchaseRequisitionId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Returns a purchase requisition.
     */
    public InspectPurchaseRequisitionWorkflowResponse inspectPurchaseRequisitionWorkflow(InspectPurchaseRequisitionWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectPurchaseRequisitionWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseRequisition", request.getPurchaseRequisitionId(), "InspectPurchaseRequisitionWorkflow");

        var builder0 = ApproveAssetTransferRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDetails(ApproveAssetTransferRequest.Details.newBuilder()
                .setApprover(UserRef.newBuilder()
                        .setEmail(request.getContext().getActor().getEmail())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = assetsAssetTransferApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approveAssetTransfer(outbound0);
        return InspectPurchaseRequisitionWorkflowResponse.newBuilder()
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getAssetTransfer().getBillingAddress().getCountryCode())
                                .setRecipient(reply0.getAssetTransfer().getBillingAddress().getRecipient())
                                .build())
                        .setPurchaseRequisitionId(request.getPurchaseRequisitionId())
                        .build())
                .build();
    }

    /**
     * Lists purchase requisitions a page at a time.
     */
    public ListPurchaseRequisitionWorkflowsResponse listPurchaseRequisitionWorkflows(ListPurchaseRequisitionWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPurchaseRequisitionWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseRequisition", request.getContext().getTraceId(), "ListPurchaseRequisitionWorkflows");

        var outbound0 = CancelBankAccountWorkflowRequest.newBuilder()
                .setBankAccount(BankAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .build())
                .setContext(request.getContext())
                .setDetails(CancelBankAccountWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> bankingBankAccountWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cancelBankAccountWorkflow(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPurchaseRequisitionWorkflows", request.getPage().getPageToken());
        return ListPurchaseRequisitionWorkflowsResponse.newBuilder()
                .addPurchaseRequisitions(PurchaseRequisition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getBankAccount().getAudit().getEtag())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Signals a purchase requisition.
     */
    public SignalPurchaseRequisitionWorkflowResponse signalPurchaseRequisitionWorkflow(SignalPurchaseRequisitionWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalPurchaseRequisitionWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseRequisition(request.getContext().getTenantId(), request.getPurchaseRequisition());

        var outbound0 = ScoreCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScoreCustomerRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getPurchaseRequisition().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<ScoreCustomerRequest, ScoreCustomerResponse>() {
            @Override
            public ScoreCustomerResponse apply(ScoreCustomerRequest input) {
                return customersCustomerValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreCustomer(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = CreateAppointmentRequest.newBuilder()
                .setAppointment(Appointment.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setCountryCode(request.getPurchaseRequisition().getBillingAddress().getCountryCode())
                                .setPostalCode(request.getPurchaseRequisition().getBillingAddress().getPostalCode())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(reply0.getCustomer().getRelationshipManager().getEmail())
                                .build())
                        .addParticipants(AppointmentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(reply0.getCustomer().getContact().getEmail())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel1 = channels.lookup("acme.appointments.v1.AppointmentService");
        var selected1 = AppointmentServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.createAppointment(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPurchaseRequisition().getPurchaseRequisitionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalPurchaseRequisitionWorkflow", request.getPurchaseRequisition().getPurchaseRequisitionId());
        return SignalPurchaseRequisitionWorkflowResponse.newBuilder()
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .addLines(PurchaseRequisitionLine.newBuilder()
                                .putAllAttributes(reply0.getCustomer().getAttributesMap())
                                .build())
                        .setNotes(request.getPurchaseRequisition().getNotes())
                        .setNumber(reply0.getCustomer().getPassport().getNumber())
                        .setPurchaseRequisitionId(request.getPurchaseRequisitionId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }
}
