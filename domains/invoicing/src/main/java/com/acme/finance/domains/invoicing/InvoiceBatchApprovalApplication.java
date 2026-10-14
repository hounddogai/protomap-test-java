package com.acme.finance.domains.invoicing;

import com.acme.audiences.v1.AudienceServiceGrpc;
import com.acme.audiences.v1.BatchGetAudiencesRequest;
import com.acme.audiences.v1.BatchGetAudiencesResponse;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailCardSettlement;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.RevertAuditTrailRequest;
import com.acme.audit.v1.RevertAuditTrailResponse;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PaymentPromiseWorkflowServiceGrpc;
import com.acme.collections.v1.RetryPaymentPromiseWorkflowRequest;
import com.acme.collections.v1.RetryPaymentPromiseWorkflowResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CalculatePreferenceRequest;
import com.acme.customers.v1.CalculatePreferenceResponse;
import com.acme.customers.v1.CreateCustomerRequest;
import com.acme.customers.v1.CreateCustomerResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerServiceGrpc;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.FindHouseholdsRequest;
import com.acme.customers.v1.FindHouseholdsResponse;
import com.acme.customers.v1.HouseholdQueryServiceGrpc;
import com.acme.customers.v1.ScoreCustomerRequest;
import com.acme.customers.v1.ScoreCustomerResponse;
import com.acme.documents.v1.CountDocumentsRequest;
import com.acme.documents.v1.CountDocumentsResponse;
import com.acme.documents.v1.DocumentQueryServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.invoicing.v1.ApproveInvoiceBatchRequest;
import com.acme.invoicing.v1.ApproveInvoiceBatchResponse;
import com.acme.invoicing.v1.CommentInvoiceBatchApprovalRequest;
import com.acme.invoicing.v1.CommentInvoiceBatchApprovalResponse;
import com.acme.invoicing.v1.CountEInvoiceTotalsRequest;
import com.acme.invoicing.v1.CountEInvoiceTotalsResponse;
import com.acme.invoicing.v1.DelegateInvoiceBatchApprovalRequest;
import com.acme.invoicing.v1.DelegateInvoiceBatchApprovalResponse;
import com.acme.invoicing.v1.EInvoiceReportingServiceGrpc;
import com.acme.invoicing.v1.EscalateInvoiceBatchRequest;
import com.acme.invoicing.v1.EscalateInvoiceBatchResponse;
import com.acme.invoicing.v1.ExplainInvoiceBatchDecisionRequest;
import com.acme.invoicing.v1.ExplainInvoiceBatchDecisionResponse;
import com.acme.invoicing.v1.ExportInvoiceBatchesRequest;
import com.acme.invoicing.v1.ExportInvoiceBatchesResponse;
import com.acme.invoicing.v1.GetInvoiceBatchRequest;
import com.acme.invoicing.v1.GetInvoiceBatchResponse;
import com.acme.invoicing.v1.InvoiceBatch;
import com.acme.invoicing.v1.InvoiceBatchBankSettlement;
import com.acme.invoicing.v1.InvoiceBatchQueryServiceGrpc;
import com.acme.invoicing.v1.InvoiceBatchServiceGrpc;
import com.acme.invoicing.v1.InvoiceBatchValidationServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplate;
import com.acme.invoicing.v1.InvoiceTemplateCalculationServiceGrpc;
import com.acme.invoicing.v1.RecallInvoiceBatchRequest;
import com.acme.invoicing.v1.RecallInvoiceBatchResponse;
import com.acme.invoicing.v1.RejectInvoiceBatchRequest;
import com.acme.invoicing.v1.RejectInvoiceBatchResponse;
import com.acme.invoicing.v1.SimulateInvoiceTemplateRequest;
import com.acme.invoicing.v1.SimulateInvoiceTemplateResponse;
import com.acme.invoicing.v1.SubmitInvoiceBatchRequest;
import com.acme.invoicing.v1.SubmitInvoiceBatchResponse;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.SendLedgerAccountTestNoticeRequest;
import com.acme.ledger.v1.SendLedgerAccountTestNoticeResponse;
import com.acme.ledger.v1.SubscribePostingBatchUpdatesRequest;
import com.acme.ledger.v1.SubscribePostingBatchUpdatesResponse;
import com.acme.payroll.v1.SubmitTaxWithholdingRequest;
import com.acme.payroll.v1.SubmitTaxWithholdingResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.payroll.v1.TaxWithholdingApprovalServiceGrpc;
import com.acme.suppliers.v1.RetrySupplierContractWorkflowRequest;
import com.acme.suppliers.v1.RetrySupplierContractWorkflowResponse;
import com.acme.suppliers.v1.SupplierContract;
import com.acme.suppliers.v1.SupplierContractAttachment;
import com.acme.suppliers.v1.SupplierContractWorkflowServiceGrpc;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.CancelBankTransferWorkflowRequest;
import com.acme.treasury.v1.CancelBankTransferWorkflowResponse;
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
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Routes invoice batches through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class InvoiceBatchApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(InvoiceBatchApprovalApplication.class);

    private final AudienceServiceGrpc.AudienceServiceBlockingStub audiencesAudienceServiceBlocking;

    private final AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking;

    private final ChannelRegistry channels;

    private final PaymentPromiseWorkflowServiceGrpc.PaymentPromiseWorkflowServiceBlockingStub collectionsPaymentPromiseWorkflowServiceBlocking;

    private final CustomerServiceGrpc.CustomerServiceBlockingStub customersCustomerServiceBlocking;

    private final CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking;

    private final HouseholdQueryServiceGrpc.HouseholdQueryServiceBlockingStub customersHouseholdQueryServiceBlocking;

    private final DocumentQueryServiceGrpc.DocumentQueryServiceBlockingStub documentsDocumentQueryServiceBlocking;

    private final ApplicationEventPublisher events;

    private final EInvoiceReportingServiceGrpc.EInvoiceReportingServiceBlockingStub invoicingEInvoiceReportingServiceBlocking;

    private final InvoiceBatchQueryServiceGrpc.InvoiceBatchQueryServiceBlockingStub invoicingInvoiceBatchQueryServiceBlocking;

    private final InvoiceBatchServiceGrpc.InvoiceBatchServiceBlockingStub invoicingInvoiceBatchServiceBlocking;

    private final InvoiceBatchValidationServiceGrpc.InvoiceBatchValidationServiceBlockingStub invoicingInvoiceBatchValidationServiceBlocking;

    private final PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking;

    private final InvoiceBatchApprovalRequestMapper mapper;

    private final TaxWithholdingApprovalServiceGrpc.TaxWithholdingApprovalServiceBlockingStub payrollTaxWithholdingApprovalServiceBlocking;

    private final InvoicingRepository repository;

    private final Executor rpcExecutor;

    private final SupplierContractWorkflowServiceGrpc.SupplierContractWorkflowServiceBlockingStub suppliersSupplierContractWorkflowServiceBlocking;

    public InvoiceBatchApprovalApplication(
            AudienceServiceGrpc.AudienceServiceBlockingStub audiencesAudienceServiceBlocking,
            AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking,
            ChannelRegistry channels,
            PaymentPromiseWorkflowServiceGrpc.PaymentPromiseWorkflowServiceBlockingStub collectionsPaymentPromiseWorkflowServiceBlocking,
            CustomerServiceGrpc.CustomerServiceBlockingStub customersCustomerServiceBlocking,
            CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking,
            HouseholdQueryServiceGrpc.HouseholdQueryServiceBlockingStub customersHouseholdQueryServiceBlocking,
            DocumentQueryServiceGrpc.DocumentQueryServiceBlockingStub documentsDocumentQueryServiceBlocking,
            ApplicationEventPublisher events,
            EInvoiceReportingServiceGrpc.EInvoiceReportingServiceBlockingStub invoicingEInvoiceReportingServiceBlocking,
            InvoiceBatchQueryServiceGrpc.InvoiceBatchQueryServiceBlockingStub invoicingInvoiceBatchQueryServiceBlocking,
            InvoiceBatchServiceGrpc.InvoiceBatchServiceBlockingStub invoicingInvoiceBatchServiceBlocking,
            InvoiceBatchValidationServiceGrpc.InvoiceBatchValidationServiceBlockingStub invoicingInvoiceBatchValidationServiceBlocking,
            PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking,
            InvoiceBatchApprovalRequestMapper mapper,
            TaxWithholdingApprovalServiceGrpc.TaxWithholdingApprovalServiceBlockingStub payrollTaxWithholdingApprovalServiceBlocking,
            InvoicingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierContractWorkflowServiceGrpc.SupplierContractWorkflowServiceBlockingStub suppliersSupplierContractWorkflowServiceBlocking) {
        this.audiencesAudienceServiceBlocking = audiencesAudienceServiceBlocking;
        this.auditAuditTrailHistoryServiceBlocking = auditAuditTrailHistoryServiceBlocking;
        this.channels = channels;
        this.collectionsPaymentPromiseWorkflowServiceBlocking = collectionsPaymentPromiseWorkflowServiceBlocking;
        this.customersCustomerServiceBlocking = customersCustomerServiceBlocking;
        this.customersCustomerValidationServiceBlocking = customersCustomerValidationServiceBlocking;
        this.customersHouseholdQueryServiceBlocking = customersHouseholdQueryServiceBlocking;
        this.documentsDocumentQueryServiceBlocking = documentsDocumentQueryServiceBlocking;
        this.events = events;
        this.invoicingEInvoiceReportingServiceBlocking = invoicingEInvoiceReportingServiceBlocking;
        this.invoicingInvoiceBatchQueryServiceBlocking = invoicingInvoiceBatchQueryServiceBlocking;
        this.invoicingInvoiceBatchServiceBlocking = invoicingInvoiceBatchServiceBlocking;
        this.invoicingInvoiceBatchValidationServiceBlocking = invoicingInvoiceBatchValidationServiceBlocking;
        this.ledgerPostingBatchNotificationServiceBlocking = ledgerPostingBatchNotificationServiceBlocking;
        this.mapper = mapper;
        this.payrollTaxWithholdingApprovalServiceBlocking = payrollTaxWithholdingApprovalServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierContractWorkflowServiceBlocking = suppliersSupplierContractWorkflowServiceBlocking;
    }

    public record EscalateInvoiceBatchStage1Requested(CalculatePreferenceRequest request, CompletableFuture<CalculatePreferenceResponse> completion, io.grpc.Context context) {}

    public record DelegateInvoiceBatchApprovalStage2Requested(CancelBankTransferWorkflowRequest request, CompletableFuture<CancelBankTransferWorkflowResponse> completion, io.grpc.Context context) {}

    /**
     * Submits an invoice batch.
     */
    public SubmitInvoiceBatchResponse submitInvoiceBatch(SubmitInvoiceBatchRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitInvoiceBatch")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceBatch(request.getContext().getTenantId(), request.getInvoiceBatch());

        var outbound0 = mapper.submitInvoiceBatchStage0(request);
        var reply0 = documentsDocumentQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countDocuments(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getInvoiceBatch().getInvoiceBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitInvoiceBatch", request.getInvoiceBatch().getInvoiceBatchId());
        return SubmitInvoiceBatchResponse.newBuilder()
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setInvoiceBatchId(request.getInvoiceBatchId())
                        .setStatus(request.getInvoiceBatch().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Approves an invoice batch.
     */
    public ApproveInvoiceBatchResponse approveInvoiceBatch(ApproveInvoiceBatchRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveInvoiceBatch")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceBatch(request.getContext().getTenantId(), request.getInvoiceBatch());

        var outbound0 = SubmitTaxWithholdingRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubmitTaxWithholdingRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getInvoiceBatch().getAudit().getEtag())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getInvoiceBatch().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> payrollTaxWithholdingApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitTaxWithholding(outbound0), rpcExecutor);

        var outbound1 = FindHouseholdsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindHouseholdsRequest.Filter.newBuilder()
                        .setRegion(request.getInvoiceBatch().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> customersHouseholdQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findHouseholds(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getInvoiceBatch().getInvoiceBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveInvoiceBatch", request.getInvoiceBatch().getInvoiceBatchId());
        return ApproveInvoiceBatchResponse.newBuilder()
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setInvoiceBatchId(request.getInvoiceBatchId())
                        .setNumber(request.getInvoiceBatch().getNumber())
                        .build())
                .setOutcome(ApproveInvoiceBatchResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Rejects an invoice batch.
     */
    public RejectInvoiceBatchResponse rejectInvoiceBatch(RejectInvoiceBatchRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectInvoiceBatch")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceBatch(request.getContext().getTenantId(), request.getInvoiceBatch());

        var outbound0 = SubscribePostingBatchUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubscribePostingBatchUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(ledgerPostingBatchNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).subscribePostingBatchUpdates(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getInvoiceBatch().getInvoiceBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectInvoiceBatch", request.getInvoiceBatch().getInvoiceBatchId());
        return RejectInvoiceBatchResponse.newBuilder()
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setInvoiceBatchId(request.getInvoiceBatchId())
                        .setIssuedAt(request.getInvoiceBatch().getIssuedAt())
                        .setNotes(reply0.getPostingBatch().getNotes())
                        .build())
                .build();
    }

    /**
     * Escalates an invoice batch.
     */
    public EscalateInvoiceBatchResponse escalateInvoiceBatch(EscalateInvoiceBatchRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateInvoiceBatch")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceBatch(request.getContext().getTenantId(), request.getInvoiceBatch());

        var results0 = new ArrayList<RetrySupplierContractWorkflowResponse>();
        for (var element : request.getInvoiceBatch().getAttachmentsList()) {
            RetrySupplierContractWorkflowRequest itemRequest = RetrySupplierContractWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierContract(SupplierContract.newBuilder()
                        .addAttachments(SupplierContractAttachment.newBuilder()
                                .setContentType(element.getContentType())
                                .build())
                        .build())
                .build();
            results0.add(suppliersSupplierContractWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retrySupplierContractWorkflow(itemRequest));
        }
        var reply0 = results0.isEmpty() ? RetrySupplierContractWorkflowResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = CalculatePreferenceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CalculatePreferenceRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(request.getInvoiceBatch().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<CalculatePreferenceResponse>();
        events.publishEvent(new EscalateInvoiceBatchStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound2 = mapper.escalateInvoiceBatchStage2(request, reply0);
        var reply2 = collectionsPaymentPromiseWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryPaymentPromiseWorkflow(outbound2);

        var outbound3 = ScoreCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply2.getPaymentPromise().getAudit().getUpdatedAt())
                                .setUpdatedBy(reply1.getPreference().getAudit().getUpdatedBy())
                                .build())
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply2.getPaymentPromise().getLocation().getCountryCode())
                                .build())
                        .build())
                .build();
        var reply3 = Stream.of(outbound3).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(customersCustomerValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreCustomer(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getInvoiceBatch().getInvoiceBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateInvoiceBatch", request.getInvoiceBatch().getInvoiceBatchId());
        return EscalateInvoiceBatchResponse.newBuilder()
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply3.getCustomer().getPrimaryAddress().getLinesList())
                                .setLocality(reply2.getPaymentPromise().getLocation().getLocality())
                                .setRegion(reply2.getPaymentPromise().getLocation().getRegion())
                                .build())
                        .setInvoiceBatchId(request.getInvoiceBatchId())
                        .setTotal(request.getInvoiceBatch().getTotal())
                        .build())
                .setOutcome(EscalateInvoiceBatchResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Delegates an invoice batch.
     */
    public DelegateInvoiceBatchApprovalResponse delegateInvoiceBatchApproval(DelegateInvoiceBatchApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateInvoiceBatchApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceBatch(request.getContext().getTenantId(), request.getInvoiceBatch());

        var outbound0 = mapper.delegateInvoiceBatchApprovalStage0(request);
        var reply0 = invoicingInvoiceBatchServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getInvoiceBatch(outbound0);

        var outbound1 = CreateCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getInvoiceBatch().getBillingAddress().getCountryCode())
                                .addAllLines(request.getInvoiceBatch().getBillingAddress().getLinesList())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setEmail(reply0.getInvoiceBatch().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = customersCustomerServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createCustomer(outbound1);

        var outbound2 = CancelBankTransferWorkflowRequest.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply1.getCustomer().getPrimaryAddress().getPostalCode())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CancelBankTransferWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(reply1.getCustomer().getRelationshipManager().getEmail())
                                .build())
                        .build())
                .build();
        var completion2 = new CompletableFuture<CancelBankTransferWorkflowResponse>();
        events.publishEvent(new DelegateInvoiceBatchApprovalStage2Requested(outbound2, completion2, io.grpc.Context.current()));
        var reply2 = completion2.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound3 = SendLedgerAccountTestNoticeRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SendLedgerAccountTestNoticeRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .setName(reply1.getCustomer().getName())
                                .setPhone(reply1.getCustomer().getContact().getPhone())
                                .setPreferredChannel(reply1.getCustomer().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var channel3 = channels.lookup("acme.ledger.v1.LedgerAccountNotificationService");
        var serviceName3 = "acme.ledger.v1" + "." + "LedgerAccountNotificationService";
        var methodName3 = "Sen" + "dLedgerAccountTestNotice";
        var descriptor3 = MethodDescriptor.<SendLedgerAccountTestNoticeRequest, SendLedgerAccountTestNoticeResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName3, methodName3))
                .setRequestMarshaller(ProtoUtils.marshaller(SendLedgerAccountTestNoticeRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(SendLedgerAccountTestNoticeResponse.getDefaultInstance()))
                .build();
        var reply3 = ClientCalls.blockingUnaryCall(channel3, descriptor3,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound3);
        log.atDebug().addKeyValue("auditValue", request.getInvoiceBatch().getCounterparty().getCustomerId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateInvoiceBatchApproval", request.getInvoiceBatch().getCounterparty().getCustomerId());
        return DelegateInvoiceBatchApprovalResponse.newBuilder()
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply2.getBankTransfer().getAudit().getCreatedAt())
                                .setCreatedBy(reply1.getCustomer().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(reply2.getBankTransfer().getBillingAddress())
                        .setInvoiceBatchId(request.getInvoiceBatchId())
                        .putAllMetadata(reply0.getInvoiceBatch().getMetadataMap())
                        .setNumber(request.getInvoiceBatch().getNumber())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply3.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Recalls an invoice batch.
     */
    public RecallInvoiceBatchResponse recallInvoiceBatch(RecallInvoiceBatchRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallInvoiceBatch")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceBatch(request.getContext().getTenantId(), request.getInvoiceBatch());

        var outbound0 = BatchGetAudiencesRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var adapter0 = new Function<BatchGetAudiencesRequest, BatchGetAudiencesResponse>() {
            @Override
            public BatchGetAudiencesResponse apply(BatchGetAudiencesRequest input) {
                return audiencesAudienceServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetAudiences(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = SimulateInvoiceTemplateRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getInvoiceBatch().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.invoicing.v1.InvoiceTemplateCalculationService");
        var selected1 = InvoiceTemplateCalculationServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.simulateInvoiceTemplate(outbound1);

        var outbound2 = ExportInvoiceBatchesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportInvoiceBatchesRequest.Filter.newBuilder()
                        .setRegion(reply1.getInvoiceTemplate().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream2 = invoicingInvoiceBatchQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportInvoiceBatches(outbound2);
        var reply2 = ExportInvoiceBatchesResponse.getDefaultInstance();
        while (stream2.hasNext()) {
            reply2 = stream2.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getInvoiceBatch().getInvoiceBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallInvoiceBatch", request.getInvoiceBatch().getInvoiceBatchId());
        return RecallInvoiceBatchResponse.newBuilder()
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setBillingAddress(reply1.getInvoiceTemplate().getBillingAddress())
                        .setInvoiceBatchId(request.getInvoiceBatchId())
                        .build())
                .setOutcome(RecallInvoiceBatchResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Comments an invoice batch.
     */
    public CommentInvoiceBatchApprovalResponse commentInvoiceBatchApproval(CommentInvoiceBatchApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentInvoiceBatchApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceBatch(request.getContext().getTenantId(), request.getInvoiceBatch());

        var builder0 = ExplainInvoiceBatchDecisionRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setInvoiceBatch(InvoiceBatch.newBuilder()
                .setBank(InvoiceBatchBankSettlement.newBuilder()
                        .setBic(request.getInvoiceBatch().getBank().getBic())
                        .build())
                .setBillingAddress(PostalAddress.newBuilder()
                        .setCountryCode(request.getInvoiceBatch().getBillingAddress().getCountryCode())
                        .addAllLines(request.getInvoiceBatch().getBillingAddress().getLinesList())
                        .build())
                .setCounterparty(CustomerRef.newBuilder()
                        .setDisplayName(request.getInvoiceBatch().getCounterparty().getDisplayName())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = invoicingInvoiceBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainInvoiceBatchDecision(outbound0);

        var outbound1 = RevertAuditTrailRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getInvoiceBatch().getAudit().getCreatedBy())
                                .setUpdatedAt(request.getInvoiceBatch().getAudit().getUpdatedAt())
                                .build())
                        .setCard(AuditTrailCardSettlement.newBuilder()
                                .setToken(switch (request.getInvoiceBatch().getSettlementCase()) {
                                    case CARD -> request.getInvoiceBatch().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RevertAuditTrailRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var reply1 = auditAuditTrailHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).revertAuditTrail(outbound1);

        var outbound2 = CountEInvoiceTotalsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountEInvoiceTotalsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply1.getAuditTrail().getAudit().getCreatedAt())
                        .setOwnerId(reply0.getInvoiceBatch().getCounterparty().getCustomerId())
                        .setRegion(reply0.getInvoiceBatch().getBillingAddress().getRegion())
                        .build())
                .build();
        final class Stage2Adapter {
            CountEInvoiceTotalsResponse fetch(CountEInvoiceTotalsRequest input) {
                return invoicingEInvoiceReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countEInvoiceTotals(input);
            }
        }
        var reply2 = new Stage2Adapter().fetch(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getInvoiceBatch().getInvoiceBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentInvoiceBatchApproval", request.getInvoiceBatch().getInvoiceBatchId());
        return CommentInvoiceBatchApprovalResponse.newBuilder()
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getInvoiceBatch().getAudit().getUpdatedBy())
                                .build())
                        .setInvoiceBatchId(request.getInvoiceBatchId())
                        .putAllMetadata(request.getInvoiceBatch().getMetadataMap())
                        .build())
                .setOutcome(CommentInvoiceBatchApprovalResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }
}
