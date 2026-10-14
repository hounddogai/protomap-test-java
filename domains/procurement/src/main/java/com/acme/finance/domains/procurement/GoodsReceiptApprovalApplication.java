package com.acme.finance.domains.procurement;

import com.acme.appointments.v1.AppointmentQueryServiceGrpc;
import com.acme.appointments.v1.CountAppointmentsRequest;
import com.acme.appointments.v1.CountAppointmentsResponse;
import com.acme.assets.v1.AssetTransferWorkflowServiceGrpc;
import com.acme.assets.v1.ListAssetTransferWorkflowsRequest;
import com.acme.assets.v1.ListAssetTransferWorkflowsResponse;
import com.acme.audit.v1.AnnotateAuditTrailVersionRequest;
import com.acme.audit.v1.AnnotateAuditTrailVersionResponse;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailCardSettlement;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.AuditTrailNotificationServiceGrpc;
import com.acme.audit.v1.RecordAuditTrailVersionRequest;
import com.acme.audit.v1.RecordAuditTrailVersionResponse;
import com.acme.audit.v1.UnsubscribeAuditTrailUpdatesRequest;
import com.acme.audit.v1.UnsubscribeAuditTrailUpdatesResponse;
import com.acme.budgeting.v1.CountForecastTotalsRequest;
import com.acme.budgeting.v1.CountForecastTotalsResponse;
import com.acme.budgeting.v1.ForecastReportingServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.Contract;
import com.acme.contracts.v1.ContractWorkflowServiceGrpc;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.RenewalParticipant;
import com.acme.contracts.v1.RenewalWorkflowServiceGrpc;
import com.acme.contracts.v1.RetryContractWorkflowRequest;
import com.acme.contracts.v1.RetryContractWorkflowResponse;
import com.acme.contracts.v1.StartRenewalWorkflowRequest;
import com.acme.contracts.v1.StartRenewalWorkflowResponse;
import com.acme.customers.v1.CalculateCustomerSegmentRequest;
import com.acme.customers.v1.CalculateCustomerSegmentResponse;
import com.acme.customers.v1.CloneCustomerRequest;
import com.acme.customers.v1.CloneCustomerResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordCalculationServiceGrpc;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerServiceGrpc;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.RecalculateConsentRecordsRequest;
import com.acme.customers.v1.RecalculateConsentRecordsResponse;
import com.acme.customers.v1.ScoreCustomerRequest;
import com.acme.customers.v1.ScoreCustomerResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.giftcards.v1.CountGiftCardsRequest;
import com.acme.giftcards.v1.CountGiftCardsResponse;
import com.acme.giftcards.v1.GiftCardQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplicationRef;
import com.acme.lending.v1.LoanLimit;
import com.acme.lending.v1.LoanWorkflowServiceGrpc;
import com.acme.lending.v1.PauseLoanWorkflowRequest;
import com.acme.lending.v1.PauseLoanWorkflowResponse;
import com.acme.procurement.v1.ApproveGoodsReceiptRequest;
import com.acme.procurement.v1.ApproveGoodsReceiptResponse;
import com.acme.procurement.v1.CommentGoodsReceiptApprovalRequest;
import com.acme.procurement.v1.CommentGoodsReceiptApprovalResponse;
import com.acme.procurement.v1.DelegateGoodsReceiptApprovalRequest;
import com.acme.procurement.v1.DelegateGoodsReceiptApprovalResponse;
import com.acme.procurement.v1.EscalateGoodsReceiptRequest;
import com.acme.procurement.v1.EscalateGoodsReceiptResponse;
import com.acme.procurement.v1.GetGoodsReceiptApprovalStatusRequest;
import com.acme.procurement.v1.GetGoodsReceiptApprovalStatusResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.GoodsReceiptLifecycleServiceGrpc;
import com.acme.procurement.v1.InspectPurchaseRequisitionWorkflowRequest;
import com.acme.procurement.v1.InspectPurchaseRequisitionWorkflowResponse;
import com.acme.procurement.v1.ListPendingGoodsReceiptsRequest;
import com.acme.procurement.v1.ListPendingGoodsReceiptsResponse;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionCardSettlement;
import com.acme.procurement.v1.PurchaseRequisitionLine;
import com.acme.procurement.v1.PurchaseRequisitionRef;
import com.acme.procurement.v1.PurchaseRequisitionServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionWorkflowServiceGrpc;
import com.acme.procurement.v1.RecallGoodsReceiptRequest;
import com.acme.procurement.v1.RecallGoodsReceiptResponse;
import com.acme.procurement.v1.ReinstateGoodsReceiptRequest;
import com.acme.procurement.v1.ReinstateGoodsReceiptResponse;
import com.acme.procurement.v1.RejectGoodsReceiptRequest;
import com.acme.procurement.v1.RejectGoodsReceiptResponse;
import com.acme.procurement.v1.SubmitGoodsReceiptRequest;
import com.acme.procurement.v1.SubmitGoodsReceiptResponse;
import com.acme.procurement.v1.UpdatePurchaseRequisitionRequest;
import com.acme.procurement.v1.UpdatePurchaseRequisitionResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
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
 * Routes goods receipts through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class GoodsReceiptApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(GoodsReceiptApprovalApplication.class);

    private final AppointmentQueryServiceGrpc.AppointmentQueryServiceBlockingStub appointmentsAppointmentQueryServiceBlocking;

    private final AssetTransferWorkflowServiceGrpc.AssetTransferWorkflowServiceBlockingStub assetsAssetTransferWorkflowServiceBlocking;

    private final AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking;

    private final AuditTrailNotificationServiceGrpc.AuditTrailNotificationServiceBlockingStub auditAuditTrailNotificationServiceBlocking;

    private final ForecastReportingServiceGrpc.ForecastReportingServiceBlockingStub budgetingForecastReportingServiceBlocking;

    private final ChannelRegistry channels;

    private final ContractWorkflowServiceGrpc.ContractWorkflowServiceBlockingStub contractsContractWorkflowServiceBlocking;

    private final RenewalWorkflowServiceGrpc.RenewalWorkflowServiceBlockingStub contractsRenewalWorkflowServiceBlocking;

    private final ConsentRecordCalculationServiceGrpc.ConsentRecordCalculationServiceFutureStub customersConsentRecordCalculationServiceFuture;

    private final CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking;

    private final CustomerServiceGrpc.CustomerServiceBlockingStub customersCustomerServiceBlocking;

    private final CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking;

    private final GiftCardQueryServiceGrpc.GiftCardQueryServiceBlockingStub giftcardsGiftCardQueryServiceBlocking;

    private final LoanWorkflowServiceGrpc.LoanWorkflowServiceBlockingStub lendingLoanWorkflowServiceBlocking;

    private final GoodsReceiptApprovalRequestMapper mapper;

    private final GoodsReceiptLifecycleServiceGrpc.GoodsReceiptLifecycleServiceBlockingStub procurementGoodsReceiptLifecycleServiceBlocking;

    private final PurchaseRequisitionServiceGrpc.PurchaseRequisitionServiceBlockingStub procurementPurchaseRequisitionServiceBlocking;

    private final PurchaseRequisitionWorkflowServiceGrpc.PurchaseRequisitionWorkflowServiceBlockingStub procurementPurchaseRequisitionWorkflowServiceBlocking;

    private final ProcurementRepository repository;

    private final Executor rpcExecutor;

    public GoodsReceiptApprovalApplication(
            AppointmentQueryServiceGrpc.AppointmentQueryServiceBlockingStub appointmentsAppointmentQueryServiceBlocking,
            AssetTransferWorkflowServiceGrpc.AssetTransferWorkflowServiceBlockingStub assetsAssetTransferWorkflowServiceBlocking,
            AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking,
            AuditTrailNotificationServiceGrpc.AuditTrailNotificationServiceBlockingStub auditAuditTrailNotificationServiceBlocking,
            ForecastReportingServiceGrpc.ForecastReportingServiceBlockingStub budgetingForecastReportingServiceBlocking,
            ChannelRegistry channels,
            ContractWorkflowServiceGrpc.ContractWorkflowServiceBlockingStub contractsContractWorkflowServiceBlocking,
            RenewalWorkflowServiceGrpc.RenewalWorkflowServiceBlockingStub contractsRenewalWorkflowServiceBlocking,
            ConsentRecordCalculationServiceGrpc.ConsentRecordCalculationServiceFutureStub customersConsentRecordCalculationServiceFuture,
            CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking,
            CustomerServiceGrpc.CustomerServiceBlockingStub customersCustomerServiceBlocking,
            CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking,
            GiftCardQueryServiceGrpc.GiftCardQueryServiceBlockingStub giftcardsGiftCardQueryServiceBlocking,
            LoanWorkflowServiceGrpc.LoanWorkflowServiceBlockingStub lendingLoanWorkflowServiceBlocking,
            GoodsReceiptApprovalRequestMapper mapper,
            GoodsReceiptLifecycleServiceGrpc.GoodsReceiptLifecycleServiceBlockingStub procurementGoodsReceiptLifecycleServiceBlocking,
            PurchaseRequisitionServiceGrpc.PurchaseRequisitionServiceBlockingStub procurementPurchaseRequisitionServiceBlocking,
            PurchaseRequisitionWorkflowServiceGrpc.PurchaseRequisitionWorkflowServiceBlockingStub procurementPurchaseRequisitionWorkflowServiceBlocking,
            ProcurementRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.appointmentsAppointmentQueryServiceBlocking = appointmentsAppointmentQueryServiceBlocking;
        this.assetsAssetTransferWorkflowServiceBlocking = assetsAssetTransferWorkflowServiceBlocking;
        this.auditAuditTrailHistoryServiceBlocking = auditAuditTrailHistoryServiceBlocking;
        this.auditAuditTrailNotificationServiceBlocking = auditAuditTrailNotificationServiceBlocking;
        this.budgetingForecastReportingServiceBlocking = budgetingForecastReportingServiceBlocking;
        this.channels = channels;
        this.contractsContractWorkflowServiceBlocking = contractsContractWorkflowServiceBlocking;
        this.contractsRenewalWorkflowServiceBlocking = contractsRenewalWorkflowServiceBlocking;
        this.customersConsentRecordCalculationServiceFuture = customersConsentRecordCalculationServiceFuture;
        this.customersCustomerSegmentCalculationServiceBlocking = customersCustomerSegmentCalculationServiceBlocking;
        this.customersCustomerServiceBlocking = customersCustomerServiceBlocking;
        this.customersCustomerValidationServiceBlocking = customersCustomerValidationServiceBlocking;
        this.giftcardsGiftCardQueryServiceBlocking = giftcardsGiftCardQueryServiceBlocking;
        this.lendingLoanWorkflowServiceBlocking = lendingLoanWorkflowServiceBlocking;
        this.mapper = mapper;
        this.procurementGoodsReceiptLifecycleServiceBlocking = procurementGoodsReceiptLifecycleServiceBlocking;
        this.procurementPurchaseRequisitionServiceBlocking = procurementPurchaseRequisitionServiceBlocking;
        this.procurementPurchaseRequisitionWorkflowServiceBlocking = procurementPurchaseRequisitionWorkflowServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Submits a goods receipt.
     */
    public SubmitGoodsReceiptResponse submitGoodsReceipt(SubmitGoodsReceiptRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitGoodsReceipt")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveGoodsReceipt(request.getContext().getTenantId(), request.getGoodsReceipt());

        var results0 = new ArrayList<CountGiftCardsResponse>();
        for (var element : request.getGoodsReceipt().getLinesList()) {
            CountGiftCardsRequest itemRequest = CountGiftCardsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountGiftCardsRequest.Filter.newBuilder()
                        .putAllLabels(element.getAttributesMap())
                        .setOwnerId(element.getLineId())
                        .setRegion(request.getGoodsReceipt().getBillingAddress().getRegion())
                        .build())
                .build();
            results0.add(giftcardsGiftCardQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countGiftCards(itemRequest));
        }
        var reply0 = results0.isEmpty() ? CountGiftCardsResponse.getDefaultInstance()
                : results0.getLast();
        log.atDebug().addKeyValue("auditValue", request.getGoodsReceipt().getGoodsReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitGoodsReceipt", request.getGoodsReceipt().getGoodsReceiptId());
        return SubmitGoodsReceiptResponse.newBuilder()
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setGoodsReceiptId(request.getGoodsReceiptId())
                        .build())
                .setOutcome(SubmitGoodsReceiptResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Approves a goods receipt.
     */
    public ApproveGoodsReceiptResponse approveGoodsReceipt(ApproveGoodsReceiptRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveGoodsReceipt")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveGoodsReceipt(request.getContext().getTenantId(), request.getGoodsReceipt());

        var outbound0 = CloneCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getGoodsReceipt().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.customers.v1.CustomerService/CloneCustomer", outbound0);
        var reply0 = customersCustomerServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneCustomer(command0.payload());

        var outbound1 = RecordAuditTrailVersionRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getGoodsReceipt().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getGoodsReceipt().getBillingAddress().getPostalCode())
                                .build())
                        .setCard(AuditTrailCardSettlement.newBuilder()
                                .setNetwork(request.getGoodsReceipt().getCard().getNetwork())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RecordAuditTrailVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var channel1 = channels.lookup("acme.audit.v1.AuditTrailHistoryService");
        var serviceName1 = "acme.audit.v1" + "." + "AuditTrailHistoryService";
        var methodName1 = "Rec" + "ordAuditTrailVersion";
        var descriptor1 = MethodDescriptor.<RecordAuditTrailVersionRequest, RecordAuditTrailVersionResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(RecordAuditTrailVersionRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(RecordAuditTrailVersionResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);

        var outbound2 = RpcSupport.unpack(Any.pack(ListAssetTransferWorkflowsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAssetTransferWorkflowsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getCustomer().getAudit().getCreatedAt())
                        .setRegion(reply0.getCustomer().getPrimaryAddress().getRegion())
                        .build())
                .build()), ListAssetTransferWorkflowsRequest.class);
        var reply2 = assetsAssetTransferWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listAssetTransferWorkflows(outbound2);

        var outbound3 = StartRenewalWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setRenewal(Renewal.newBuilder()
                        .addParticipants(RenewalParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setPreferredChannel(reply0.getCustomer().getContact().getPreferredChannel())
                                        .build())
                                .build())
                        .build())
                .build();
        RpcPort<StartRenewalWorkflowRequest, StartRenewalWorkflowResponse> port3 = RpcPort.adapt(contractsRenewalWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::startRenewalWorkflow);
        var reply3 = port3.exchange(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getGoodsReceipt().getGoodsReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveGoodsReceipt", request.getGoodsReceipt().getGoodsReceiptId());
        return ApproveGoodsReceiptResponse.newBuilder()
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply3.getRenewal().getLocation().getPostalCode())
                                .build())
                        .setCounterparty(PurchaseRequisitionRef.newBuilder()
                                .setNumber(reply1.getAuditTrail().getNumber())
                                .build())
                        .setGoodsReceiptId(request.getGoodsReceiptId())
                        .setNotes(request.getGoodsReceipt().getNotes())
                        .build())
                .setOutcome(ApproveGoodsReceiptResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply3.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Rejects a goods receipt.
     */
    public RejectGoodsReceiptResponse rejectGoodsReceipt(RejectGoodsReceiptRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectGoodsReceipt")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveGoodsReceipt(request.getContext().getTenantId(), request.getGoodsReceipt());

        var futures0 = request.getGoodsReceipt().getLinesList().stream()
                .map(element -> PauseLoanWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setLoan(Loan.newBuilder()
                        .setHolder(LoanApplicationRef.newBuilder()
                                .setDisplayName(request.getGoodsReceipt().getCounterparty().getDisplayName())
                                .build())
                        .addLimits(LoanLimit.newBuilder()
                                .setAmount(element.getTax().getAmount())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> lendingLoanWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pauseLoanWorkflow(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? PauseLoanWorkflowResponse.getDefaultInstance()
                : results0.getLast();

        var builder1 = CountAppointmentsRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setFilter(CountAppointmentsRequest.Filter.newBuilder()
                .setRegion(request.getGoodsReceipt().getBillingAddress().getRegion())
                .build());
        var outbound1 = builder1.build();
        var reply1 = appointmentsAppointmentQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countAppointments(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getGoodsReceipt().getGoodsReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectGoodsReceipt", request.getGoodsReceipt().getGoodsReceiptId());
        return RejectGoodsReceiptResponse.newBuilder()
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setCounterparty(request.getGoodsReceipt().getCounterparty())
                        .setGoodsReceiptId(request.getGoodsReceiptId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Escalates a goods receipt.
     */
    public EscalateGoodsReceiptResponse escalateGoodsReceipt(EscalateGoodsReceiptRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateGoodsReceipt")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveGoodsReceipt(request.getContext().getTenantId(), request.getGoodsReceipt());

        var outbound0 = ScoreCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getGoodsReceipt().getBillingAddress().getPostalCode())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setDisplayName(request.getGoodsReceipt().getCounterparty().getDisplayName())
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .setDetails(ScoreCustomerRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getGoodsReceipt().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .build();
        Function<ScoreCustomerRequest, ScoreCustomerResponse> operation0 = customersCustomerValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::scoreCustomer;
        var reply0 = operation0.apply(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getGoodsReceipt().getGoodsReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateGoodsReceipt", request.getGoodsReceipt().getGoodsReceiptId());
        return EscalateGoodsReceiptResponse.newBuilder()
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setCounterparty(PurchaseRequisitionRef.newBuilder()
                                .setNumber(reply0.getCustomer().getNationalId().getNumber())
                                .build())
                        .setGoodsReceiptId(request.getGoodsReceiptId())
                        .setNotes(request.getGoodsReceipt().getNotes())
                        .setNumber(reply0.getCustomer().getNationalId().getNumber())
                        .build())
                .build();
    }

    /**
     * Lists goods receipts a page at a time.
     */
    public ListPendingGoodsReceiptsResponse listPendingGoodsReceipts(ListPendingGoodsReceiptsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingGoodsReceipts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "GoodsReceipt", request.getContext().getTraceId(), "ListPendingGoodsReceipts");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingGoodsReceipts", request.getPage().getPageToken());
        return ListPendingGoodsReceiptsResponse.getDefaultInstance();
    }

    /**
     * Delegates a goods receipt.
     */
    public DelegateGoodsReceiptApprovalResponse delegateGoodsReceiptApproval(DelegateGoodsReceiptApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateGoodsReceiptApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveGoodsReceipt(request.getContext().getTenantId(), request.getGoodsReceipt());

        List<UnsubscribeAuditTrailUpdatesResponse> results0 = request.getGoodsReceipt().getLinesList().stream()
                .map(element -> UnsubscribeAuditTrailUpdatesRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(UnsubscribeAuditTrailUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .putAllVariables(element.getAttributesMap())
                        .build())
                .setReason(request.getReason())
                .build())
                .flatMap(value -> Stream.of(auditAuditTrailNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).unsubscribeAuditTrailUpdates(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? UnsubscribeAuditTrailUpdatesResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = (AnnotateAuditTrailVersionRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getAuditTrail().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(AnnotateAuditTrailVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .build()).toBuilder().build();
        var reply1 = auditAuditTrailHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).annotateAuditTrailVersion(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getGoodsReceipt().getGoodsReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateGoodsReceiptApproval", request.getGoodsReceipt().getGoodsReceiptId());
        return DelegateGoodsReceiptApprovalResponse.newBuilder()
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply1.getAuditTrail().getBillingAddress().getRegion())
                                .build())
                        .setGoodsReceiptId(request.getGoodsReceiptId())
                        .setNumber(request.getGoodsReceipt().getNumber())
                        .setTotal(reply1.getAuditTrail().getTotal())
                        .build())
                .build();
    }

    /**
     * Recalls a goods receipt.
     */
    public RecallGoodsReceiptResponse recallGoodsReceipt(RecallGoodsReceiptRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallGoodsReceipt")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveGoodsReceipt(request.getContext().getTenantId(), request.getGoodsReceipt());

        var builder0 = ReinstateGoodsReceiptRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDetails(ReinstateGoodsReceiptRequest.Details.newBuilder()
                .setNewOwner(UserRef.newBuilder()
                        .setDisplayName(request.getDetails().getApprover().getDisplayName())
                        .setEmail(request.getDetails().getApprover().getEmail())
                        .setUserId(request.getDetails().getApprover().getUserId())
                        .build())
                .build());
        builder0.setGoodsReceiptId(request.getGoodsReceipt().getGoodsReceiptId());
        builder0.setReason(request.getReason());
        var outbound0 = builder0.build();
        var reply0 = procurementGoodsReceiptLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).reinstateGoodsReceipt(outbound0);

        var outbound1 = mapper.recallGoodsReceiptStage1(request);
        var reply1 = contractsContractWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryContractWorkflow(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getGoodsReceipt().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallGoodsReceipt", request.getGoodsReceipt().getNumber());
        return RecallGoodsReceiptResponse.newBuilder()
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setCounterparty(PurchaseRequisitionRef.newBuilder()
                                .setPurchaseRequisitionId(reply0.getGoodsReceipt().getCounterparty().getPurchaseRequisitionId())
                                .build())
                        .setGoodsReceiptId(request.getGoodsReceiptId())
                        .setTotal(request.getGoodsReceipt().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Returns a goods receipt.
     */
    public GetGoodsReceiptApprovalStatusResponse getGoodsReceiptApprovalStatus(GetGoodsReceiptApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetGoodsReceiptApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "GoodsReceipt", request.getGoodsReceiptId(), "GetGoodsReceiptApprovalStatus");

        var outbound0 = CountForecastTotalsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountForecastTotalsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .putAllLabels(request.getContext().getBaggageMap())
                        .setOwnerId(request.getGoodsReceiptId())
                        .build())
                .build();
        final class Stage0Adapter {
            CountForecastTotalsResponse fetch(CountForecastTotalsRequest input) {
                return budgetingForecastReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countForecastTotals(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        return GetGoodsReceiptApprovalStatusResponse.newBuilder()
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setGoodsReceiptId(request.getGoodsReceiptId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Comments a goods receipt.
     */
    public CommentGoodsReceiptApprovalResponse commentGoodsReceiptApproval(CommentGoodsReceiptApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentGoodsReceiptApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveGoodsReceipt(request.getContext().getTenantId(), request.getGoodsReceipt());

        var outbound0 = CalculateCustomerSegmentRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CalculateCustomerSegmentRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(request.getGoodsReceipt().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> customersCustomerSegmentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateCustomerSegment(outbound0), rpcExecutor);

        var outbound1 = InspectPurchaseRequisitionWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseRequisitionId(request.getGoodsReceipt().getCounterparty().getPurchaseRequisitionId())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> procurementPurchaseRequisitionWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).inspectPurchaseRequisitionWorkflow(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = RecalculateConsentRecordsRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .build())
                .setContext(request.getContext())
                .setDetails(RecalculateConsentRecordsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(reply1.getPurchaseRequisition().getBillingAddress().getLocality())
                                .build())
                        .build())
                .build();
        var pending2 = customersConsentRecordCalculationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateConsentRecords(outbound2);
        var reply2 = RpcSupport.await(pending2);

        var outbound3 = UpdatePurchaseRequisitionRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getGoodsReceipt().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply1.getPurchaseRequisition().getBillingAddress().getLinesList())
                                .build())
                        .setCard(PurchaseRequisitionCardSettlement.newBuilder()
                                .setHolderName(request.getGoodsReceipt().getCard().getHolderName())
                                .build())
                        .addLines(PurchaseRequisitionLine.newBuilder()
                                .setAmount(reply0.getCustomerSegment().getDiscount().getAmount())
                                .build())
                        .build())
                .build();
        var adapter3 = new Function<UpdatePurchaseRequisitionRequest, UpdatePurchaseRequisitionResponse>() {
            @Override
            public UpdatePurchaseRequisitionResponse apply(UpdatePurchaseRequisitionRequest input) {
                return procurementPurchaseRequisitionServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updatePurchaseRequisition(input);
            }
        };
        var reply3 = adapter3.apply(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getGoodsReceipt().getGoodsReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentGoodsReceiptApproval", request.getGoodsReceipt().getGoodsReceiptId());
        return CommentGoodsReceiptApprovalResponse.newBuilder()
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getPurchaseRequisition().getBillingAddress().getCountryCode())
                                .build())
                        .setGoodsReceiptId(request.getGoodsReceiptId())
                        .setIssuedAt(request.getGoodsReceipt().getIssuedAt())
                        .setTotal(reply1.getPurchaseRequisition().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }
}
