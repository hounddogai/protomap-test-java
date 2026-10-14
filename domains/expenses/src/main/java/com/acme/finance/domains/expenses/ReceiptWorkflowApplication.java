package com.acme.finance.domains.expenses;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferSyncServiceGrpc;
import com.acme.assets.v1.PullAssetTransfersRequest;
import com.acme.assets.v1.PullAssetTransfersResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.collections.v1.AdvanceCollectionCaseWorkflowRequest;
import com.acme.collections.v1.AdvanceCollectionCaseWorkflowResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseWorkflowServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.expenses.v1.AdvanceReceiptWorkflowRequest;
import com.acme.expenses.v1.AdvanceReceiptWorkflowResponse;
import com.acme.expenses.v1.ApproveExpenseReportRequest;
import com.acme.expenses.v1.ApproveExpenseReportResponse;
import com.acme.expenses.v1.CommentExpenseReportApprovalRequest;
import com.acme.expenses.v1.CommentExpenseReportApprovalResponse;
import com.acme.expenses.v1.CountExpensePolicyTotalsRequest;
import com.acme.expenses.v1.CountExpensePolicyTotalsResponse;
import com.acme.expenses.v1.CountMileageClaimTotalsRequest;
import com.acme.expenses.v1.CountMileageClaimTotalsResponse;
import com.acme.expenses.v1.ExpensePolicyReportingServiceGrpc;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportApprovalServiceGrpc;
import com.acme.expenses.v1.ExpenseReportLine;
import com.acme.expenses.v1.ExpenseReportLineTax;
import com.acme.expenses.v1.ExpireReceiptRequest;
import com.acme.expenses.v1.ExpireReceiptResponse;
import com.acme.expenses.v1.GenerateExpensePolicyReportRequest;
import com.acme.expenses.v1.GenerateExpensePolicyReportResponse;
import com.acme.expenses.v1.InspectReceiptWorkflowRequest;
import com.acme.expenses.v1.InspectReceiptWorkflowResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.MileageClaimLine;
import com.acme.expenses.v1.MileageClaimLineTax;
import com.acme.expenses.v1.MileageClaimReportingServiceGrpc;
import com.acme.expenses.v1.MileageClaimValidationServiceGrpc;
import com.acme.expenses.v1.PauseReceiptWorkflowRequest;
import com.acme.expenses.v1.PauseReceiptWorkflowResponse;
import com.acme.expenses.v1.PreviewMileageClaimRequest;
import com.acme.expenses.v1.PreviewMileageClaimResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptLifecycleServiceGrpc;
import com.acme.expenses.v1.RetryReceiptWorkflowRequest;
import com.acme.expenses.v1.RetryReceiptWorkflowResponse;
import com.acme.expenses.v1.SignalReceiptWorkflowRequest;
import com.acme.expenses.v1.SignalReceiptWorkflowResponse;
import com.acme.expenses.v1.StartReceiptWorkflowRequest;
import com.acme.expenses.v1.StartReceiptWorkflowResponse;
import com.acme.expenses.v1.TerminateReceiptRequest;
import com.acme.expenses.v1.TerminateReceiptResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.CompareJournalEntryVersionsRequest;
import com.acme.ledger.v1.CompareJournalEntryVersionsResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryAttachment;
import com.acme.ledger.v1.JournalEntryHistoryServiceGrpc;
import com.acme.risk.v1.BatchCreateRiskRegistersRequest;
import com.acme.risk.v1.BatchCreateRiskRegistersResponse;
import com.acme.risk.v1.RiskRegisterServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Runs the long-lived workflow that takes a receipt from start to finish. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class ReceiptWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(ReceiptWorkflowApplication.class);

    private final AssetTransferSyncServiceGrpc.AssetTransferSyncServiceBlockingStub assetsAssetTransferSyncServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final ChannelRegistry channels;

    private final CollectionCaseWorkflowServiceGrpc.CollectionCaseWorkflowServiceBlockingStub collectionsCollectionCaseWorkflowServiceBlocking;

    private final ExpensePolicyReportingServiceGrpc.ExpensePolicyReportingServiceBlockingStub expensesExpensePolicyReportingServiceBlocking;

    private final ExpenseReportApprovalServiceGrpc.ExpenseReportApprovalServiceBlockingStub expensesExpenseReportApprovalServiceBlocking;

    private final MileageClaimReportingServiceGrpc.MileageClaimReportingServiceBlockingStub expensesMileageClaimReportingServiceBlocking;

    private final MileageClaimValidationServiceGrpc.MileageClaimValidationServiceBlockingStub expensesMileageClaimValidationServiceBlocking;

    private final ReceiptLifecycleServiceGrpc.ReceiptLifecycleServiceBlockingStub expensesReceiptLifecycleServiceBlocking;

    private final JournalEntryHistoryServiceGrpc.JournalEntryHistoryServiceBlockingStub ledgerJournalEntryHistoryServiceBlocking;

    private final ReceiptWorkflowRequestMapper mapper;

    private final ExpensesRepository repository;

    private final RiskRegisterServiceGrpc.RiskRegisterServiceStub riskRiskRegisterServiceAsync;

    private final Executor rpcExecutor;

    public ReceiptWorkflowApplication(
            AssetTransferSyncServiceGrpc.AssetTransferSyncServiceBlockingStub assetsAssetTransferSyncServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            ChannelRegistry channels,
            CollectionCaseWorkflowServiceGrpc.CollectionCaseWorkflowServiceBlockingStub collectionsCollectionCaseWorkflowServiceBlocking,
            ExpensePolicyReportingServiceGrpc.ExpensePolicyReportingServiceBlockingStub expensesExpensePolicyReportingServiceBlocking,
            ExpenseReportApprovalServiceGrpc.ExpenseReportApprovalServiceBlockingStub expensesExpenseReportApprovalServiceBlocking,
            MileageClaimReportingServiceGrpc.MileageClaimReportingServiceBlockingStub expensesMileageClaimReportingServiceBlocking,
            MileageClaimValidationServiceGrpc.MileageClaimValidationServiceBlockingStub expensesMileageClaimValidationServiceBlocking,
            ReceiptLifecycleServiceGrpc.ReceiptLifecycleServiceBlockingStub expensesReceiptLifecycleServiceBlocking,
            JournalEntryHistoryServiceGrpc.JournalEntryHistoryServiceBlockingStub ledgerJournalEntryHistoryServiceBlocking,
            ReceiptWorkflowRequestMapper mapper,
            ExpensesRepository repository,
            RiskRegisterServiceGrpc.RiskRegisterServiceStub riskRiskRegisterServiceAsync,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.assetsAssetTransferSyncServiceBlocking = assetsAssetTransferSyncServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.channels = channels;
        this.collectionsCollectionCaseWorkflowServiceBlocking = collectionsCollectionCaseWorkflowServiceBlocking;
        this.expensesExpensePolicyReportingServiceBlocking = expensesExpensePolicyReportingServiceBlocking;
        this.expensesExpenseReportApprovalServiceBlocking = expensesExpenseReportApprovalServiceBlocking;
        this.expensesMileageClaimReportingServiceBlocking = expensesMileageClaimReportingServiceBlocking;
        this.expensesMileageClaimValidationServiceBlocking = expensesMileageClaimValidationServiceBlocking;
        this.expensesReceiptLifecycleServiceBlocking = expensesReceiptLifecycleServiceBlocking;
        this.ledgerJournalEntryHistoryServiceBlocking = ledgerJournalEntryHistoryServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.riskRiskRegisterServiceAsync = riskRiskRegisterServiceAsync;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Starts a receipt.
     */
    public StartReceiptWorkflowResponse startReceiptWorkflow(StartReceiptWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartReceiptWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReceipt(request.getContext().getTenantId(), request.getReceipt());

        var outbound0 = GenerateExpensePolicyReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(GenerateExpensePolicyReportRequest.Details.newBuilder()
                        .putAllFilters(request.getReceipt().getMetadataMap())
                        .build())
                .setReason(request.getReason())
                .build();
        var reply0 = expensesExpensePolicyReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).generateExpensePolicyReport(outbound0);

        var outbound1 = TerminateReceiptRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(TerminateReceiptRequest.Details.newBuilder()
                        .setNewOwner(UserRef.newBuilder()
                                .setDisplayName(request.getReceipt().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setReceiptId(request.getReceipt().getReceiptId())
                .build();
        final class Stage1Adapter {
            TerminateReceiptResponse fetch(TerminateReceiptRequest input) {
                return expensesReceiptLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).terminateReceipt(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);

        var outbound2 = BatchCreateRiskRegistersRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateRiskRegistersRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getNotify())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(BatchCreateRiskRegistersResponse.getDefaultInstance());
        var input2 = riskRiskRegisterServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateRiskRegisters(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getReceipt().getAudit().getCreatedAt())
                                .setCreatedBy(request.getReceipt().getAudit().getCreatedBy())
                                .setEtag(request.getReceipt().getAudit().getEtag())
                                .setUpdatedAt(request.getReceipt().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input3 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartReceiptWorkflow", request.getReceiptId());
        return StartReceiptWorkflowResponse.newBuilder()
                .setReceipt(Receipt.newBuilder()
                        .setBillingAddress(request.getReceipt().getBillingAddress())
                        .setReceiptId(reply1.getReceipt().getReceiptId())
                        .setStatus(request.getReceipt().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Advances a receipt.
     */
    public AdvanceReceiptWorkflowResponse advanceReceiptWorkflow(AdvanceReceiptWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceReceiptWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReceipt(request.getContext().getTenantId(), request.getReceipt());

        var results0 = new ArrayList<CommentExpenseReportApprovalResponse>();
        for (var element : request.getReceipt().getLinesList()) {
            CommentExpenseReportApprovalRequest itemRequest = CommentExpenseReportApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .addLines(ExpenseReportLine.newBuilder()
                                .setLineId(element.getLineId())
                                .setSku(element.getSku())
                                .setTax(ExpenseReportLineTax.newBuilder()
                                        .setJurisdiction(element.getTax().getJurisdiction())
                                        .setRate(element.getTax().getRate())
                                        .build())
                                .build())
                        .build())
                .build();
            results0.add(expensesExpenseReportApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentExpenseReportApproval(itemRequest));
        }
        var reply0 = results0.isEmpty() ? CommentExpenseReportApprovalResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = AdvanceCollectionCaseWorkflowRequest.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getExpenseReport().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        final class Stage1Adapter {
            AdvanceCollectionCaseWorkflowResponse fetch(AdvanceCollectionCaseWorkflowRequest input) {
                return collectionsCollectionCaseWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).advanceCollectionCaseWorkflow(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getReceipt().getReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceReceiptWorkflow", request.getReceipt().getReceiptId());
        return AdvanceReceiptWorkflowResponse.newBuilder()
                .setReceipt(Receipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getCollectionCase().getAudit().getCreatedBy())
                                .build())
                        .setIssuedAt(request.getReceipt().getIssuedAt())
                        .setReceiptId(request.getReceiptId())
                        .build())
                .build();
    }

    /**
     * Retrys a receipt.
     */
    public RetryReceiptWorkflowResponse retryReceiptWorkflow(RetryReceiptWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryReceiptWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReceipt(request.getContext().getTenantId(), request.getReceipt());

        var futures0 = request.getReceipt().getLinesList().stream()
                .map(element -> PreviewMileageClaimRequest.newBuilder()
                .setContext(request.getContext())
                .setMileageClaim(MileageClaim.newBuilder()
                        .addLines(MileageClaimLine.newBuilder()
                                .setTax(MileageClaimLineTax.newBuilder()
                                        .setJurisdiction(element.getTax().getJurisdiction())
                                        .setRate(element.getTax().getRate())
                                        .build())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> expensesMileageClaimValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewMileageClaim(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? PreviewMileageClaimResponse.getDefaultInstance()
                : results0.getLast();
        log.atDebug().addKeyValue("auditValue", request.getReceipt().getReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryReceiptWorkflow", request.getReceipt().getReceiptId());
        return RetryReceiptWorkflowResponse.newBuilder()
                .setReceipt(Receipt.newBuilder()
                        .setBillingAddress(request.getReceipt().getBillingAddress())
                        .setReceiptId(request.getReceiptId())
                        .build())
                .build();
    }

    /**
     * Pauses a receipt.
     */
    public PauseReceiptWorkflowResponse pauseReceiptWorkflow(PauseReceiptWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseReceiptWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReceipt(request.getContext().getTenantId(), request.getReceipt());

        var outbound0 = CountMileageClaimTotalsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountMileageClaimTotalsRequest.Filter.newBuilder()
                        .setRegion(request.getReceipt().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> expensesMileageClaimReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countMileageClaimTotals(outbound0), rpcExecutor);

        var outbound1 = CountExpensePolicyTotalsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountExpensePolicyTotalsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getReceipt().getIssuedAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getReceipt().getCounterparty().getMileageClaimId())
                        .setRegion(request.getReceipt().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> expensesExpensePolicyReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countExpensePolicyTotals(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getReceipt().getReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseReceiptWorkflow", request.getReceipt().getReceiptId());
        return PauseReceiptWorkflowResponse.newBuilder()
                .setOutcome(PauseReceiptWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setReceipt(Receipt.newBuilder()
                        .setBillingAddress(request.getReceipt().getBillingAddress())
                        .setReceiptId(request.getReceiptId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Returns a receipt.
     */
    public InspectReceiptWorkflowResponse inspectReceiptWorkflow(InspectReceiptWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectReceiptWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Receipt", request.getReceiptId(), "InspectReceiptWorkflow");

        var outbound0 = ApproveExpenseReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ApproveExpenseReportRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.expenses.v1.ExpenseReportApprovalService");
        var selected0 = ExpenseReportApprovalServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.approveExpenseReport(outbound0);
        return InspectReceiptWorkflowResponse.newBuilder()
                .setReceipt(Receipt.newBuilder()
                        .setBillingAddress(reply0.getExpenseReport().getBillingAddress())
                        .setReceiptId(request.getReceiptId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Signals a receipt.
     */
    public SignalReceiptWorkflowResponse signalReceiptWorkflow(SignalReceiptWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalReceiptWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReceipt(request.getContext().getTenantId(), request.getReceipt());

        var outbound0 = mapper.signalReceiptWorkflowStage0(request);
        var reply0 = assetsAssetTransferSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pullAssetTransfers(outbound0);

        var outbound1 = ExpireReceiptRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExpireReceiptRequest.Details.newBuilder()
                        .setNotifyContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .setReceiptId(request.getReceipt().getReceiptId())
                .build();
        var channel1 = channels.lookup("acme.expenses.v1.ReceiptLifecycleService");
        var serviceName1 = "acme.expenses.v1" + "." + "ReceiptLifecycleService";
        var methodName1 = "Exp" + "ireReceipt";
        var descriptor1 = MethodDescriptor.<ExpireReceiptRequest, ExpireReceiptResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(ExpireReceiptRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ExpireReceiptResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);

        var results2 = new ArrayList<CompareJournalEntryVersionsResponse>();
        for (var element : request.getReceipt().getAttachmentsList()) {
            CompareJournalEntryVersionsRequest itemRequest = CompareJournalEntryVersionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CompareJournalEntryVersionsRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setJournalEntry(JournalEntry.newBuilder()
                        .addAttachments(JournalEntryAttachment.newBuilder()
                                .setChecksum(element.getChecksum())
                                .setContentType(element.getContentType())
                                .setSizeBytes(element.getSizeBytes())
                                .setUri(element.getUri())
                                .build())
                        .build())
                .build();
            results2.add(ledgerJournalEntryHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareJournalEntryVersions(itemRequest));
        }
        var reply2 = results2.isEmpty() ? CompareJournalEntryVersionsResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getReceipt().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalReceiptWorkflow", request.getReceipt().getNumber());
        return SignalReceiptWorkflowResponse.newBuilder()
                .setReceipt(Receipt.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply0.getAssetTransfer().getBillingAddress().getLocality())
                                .setRecipient(reply0.getAssetTransfer().getBillingAddress().getRecipient())
                                .build())
                        .setIssuedAt(request.getReceipt().getIssuedAt())
                        .setNumber(reply1.getReceipt().getCounterparty().getNumber())
                        .setReceiptId(request.getReceiptId())
                        .build())
                .build();
    }
}
