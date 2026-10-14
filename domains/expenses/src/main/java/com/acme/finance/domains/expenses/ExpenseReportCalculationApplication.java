package com.acme.finance.domains.expenses;

import com.acme.assets.v1.BatchCreateFixedAssetsRequest;
import com.acme.assets.v1.BatchCreateFixedAssetsResponse;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalCalculationServiceGrpc;
import com.acme.assets.v1.DisposalComment;
import com.acme.assets.v1.ExplainDisposalCalculationRequest;
import com.acme.assets.v1.ExplainDisposalCalculationResponse;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.audit.v1.NormalizeRetentionPolicyRequest;
import com.acme.audit.v1.NormalizeRetentionPolicyResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyDiscountAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.RetentionPolicyValidationServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.chat.v1.ChatRoom;
import com.acme.chat.v1.ChatRoomServiceGrpc;
import com.acme.chat.v1.CreateChatRoomRequest;
import com.acme.chat.v1.CreateChatRoomResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.expenses.v1.CalculateExpenseReportRequest;
import com.acme.expenses.v1.CalculateExpenseReportResponse;
import com.acme.expenses.v1.CompareExpenseReportOptionsRequest;
import com.acme.expenses.v1.CompareExpenseReportOptionsResponse;
import com.acme.expenses.v1.EstimateExpenseReportRequest;
import com.acme.expenses.v1.EstimateExpenseReportResponse;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExplainExpenseReportCalculationRequest;
import com.acme.expenses.v1.ExplainExpenseReportCalculationResponse;
import com.acme.expenses.v1.GetExpenseReportRatesRequest;
import com.acme.expenses.v1.GetExpenseReportRatesResponse;
import com.acme.expenses.v1.QuoteExpenseReportRequest;
import com.acme.expenses.v1.QuoteExpenseReportResponse;
import com.acme.expenses.v1.RecalculateExpenseReportsRequest;
import com.acme.expenses.v1.RecalculateExpenseReportsResponse;
import com.acme.expenses.v1.SimulateExpenseReportRequest;
import com.acme.expenses.v1.SimulateExpenseReportResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.labeling.v1.BatchGetLabelingTasksRequest;
import com.acme.labeling.v1.BatchGetLabelingTasksResponse;
import com.acme.labeling.v1.LabelingTaskServiceGrpc;
import com.acme.ledger.v1.CloneLedgerAccountRequest;
import com.acme.ledger.v1.CloneLedgerAccountResponse;
import com.acme.ledger.v1.CompareJournalEntryVersionsRequest;
import com.acme.ledger.v1.CompareJournalEntryVersionsResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryAttachment;
import com.acme.ledger.v1.JournalEntryHistoryServiceGrpc;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountCardFunding;
import com.acme.ledger.v1.LedgerAccountServiceGrpc;
import com.acme.lending.v1.CheckRepaymentEligibilityRequest;
import com.acme.lending.v1.CheckRepaymentEligibilityResponse;
import com.acme.lending.v1.ExplainRepaymentDecisionRequest;
import com.acme.lending.v1.ExplainRepaymentDecisionResponse;
import com.acme.lending.v1.PreviewRepaymentRequest;
import com.acme.lending.v1.PreviewRepaymentResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.RepaymentValidationServiceGrpc;
import com.acme.lending.v1.RepaymentWeeklyRule;
import com.acme.lending.v1.ScoreRepaymentRequest;
import com.acme.lending.v1.ScoreRepaymentResponse;
import com.acme.notifications.v1.CountNotificationsRequest;
import com.acme.notifications.v1.CountNotificationsResponse;
import com.acme.notifications.v1.NotificationQueryServiceGrpc;
import com.acme.notifications.v1.ResolveNotificationsRequest;
import com.acme.notifications.v1.ResolveNotificationsResponse;
import com.acme.patients.v1.PatientRef;
import com.acme.prescriptions.v1.ComparePrescriptionOptionsRequest;
import com.acme.prescriptions.v1.ComparePrescriptionOptionsResponse;
import com.acme.prescriptions.v1.Prescription;
import com.acme.prescriptions.v1.PrescriptionCalculationServiceGrpc;
import com.acme.prescriptions.v1.PrescriptionCardSettlement;
import com.acme.procurement.v1.EstimateRfqRequest;
import com.acme.procurement.v1.EstimateRfqResponse;
import com.acme.procurement.v1.LintPurchaseOrdersRequest;
import com.acme.procurement.v1.LintPurchaseOrdersResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderValidationServiceGrpc;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqCalculationServiceGrpc;
import com.acme.search.v1.BatchGetSearchIndexesRequest;
import com.acme.search.v1.BatchGetSearchIndexesResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.acme.search.v1.SearchIndexServiceGrpc;
import com.acme.search.v1.SuggestSearchIndexesRequest;
import com.acme.search.v1.SuggestSearchIndexesResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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
 * Computes the figures of an expense report, such as totals, taxes, and estimates. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class ExpenseReportCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(ExpenseReportCalculationApplication.class);

    private final DisposalCalculationServiceGrpc.DisposalCalculationServiceBlockingStub assetsDisposalCalculationServiceBlocking;

    private final FixedAssetServiceGrpc.FixedAssetServiceStub assetsFixedAssetServiceAsync;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final RetentionPolicyValidationServiceGrpc.RetentionPolicyValidationServiceBlockingStub auditRetentionPolicyValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final ChatRoomServiceGrpc.ChatRoomServiceBlockingStub chatChatRoomServiceBlocking;

    private final LabelingTaskServiceGrpc.LabelingTaskServiceBlockingStub labelingLabelingTaskServiceBlocking;

    private final JournalEntryHistoryServiceGrpc.JournalEntryHistoryServiceBlockingStub ledgerJournalEntryHistoryServiceBlocking;

    private final LedgerAccountServiceGrpc.LedgerAccountServiceBlockingStub ledgerLedgerAccountServiceBlocking;

    private final RepaymentValidationServiceGrpc.RepaymentValidationServiceBlockingStub lendingRepaymentValidationServiceBlocking;

    private final NotificationQueryServiceGrpc.NotificationQueryServiceBlockingStub notificationsNotificationQueryServiceBlocking;

    private final PrescriptionCalculationServiceGrpc.PrescriptionCalculationServiceBlockingStub prescriptionsPrescriptionCalculationServiceBlocking;

    private final PurchaseOrderValidationServiceGrpc.PurchaseOrderValidationServiceBlockingStub procurementPurchaseOrderValidationServiceBlocking;

    private final RfqCalculationServiceGrpc.RfqCalculationServiceBlockingStub procurementRfqCalculationServiceBlocking;

    private final ExpensesRepository repository;

    private final Executor rpcExecutor;

    private final SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking;

    private final SearchIndexServiceGrpc.SearchIndexServiceBlockingStub searchSearchIndexServiceBlocking;

    public ExpenseReportCalculationApplication(
            DisposalCalculationServiceGrpc.DisposalCalculationServiceBlockingStub assetsDisposalCalculationServiceBlocking,
            FixedAssetServiceGrpc.FixedAssetServiceStub assetsFixedAssetServiceAsync,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            RetentionPolicyValidationServiceGrpc.RetentionPolicyValidationServiceBlockingStub auditRetentionPolicyValidationServiceBlocking,
            ChannelRegistry channels,
            ChatRoomServiceGrpc.ChatRoomServiceBlockingStub chatChatRoomServiceBlocking,
            LabelingTaskServiceGrpc.LabelingTaskServiceBlockingStub labelingLabelingTaskServiceBlocking,
            JournalEntryHistoryServiceGrpc.JournalEntryHistoryServiceBlockingStub ledgerJournalEntryHistoryServiceBlocking,
            LedgerAccountServiceGrpc.LedgerAccountServiceBlockingStub ledgerLedgerAccountServiceBlocking,
            RepaymentValidationServiceGrpc.RepaymentValidationServiceBlockingStub lendingRepaymentValidationServiceBlocking,
            NotificationQueryServiceGrpc.NotificationQueryServiceBlockingStub notificationsNotificationQueryServiceBlocking,
            PrescriptionCalculationServiceGrpc.PrescriptionCalculationServiceBlockingStub prescriptionsPrescriptionCalculationServiceBlocking,
            PurchaseOrderValidationServiceGrpc.PurchaseOrderValidationServiceBlockingStub procurementPurchaseOrderValidationServiceBlocking,
            RfqCalculationServiceGrpc.RfqCalculationServiceBlockingStub procurementRfqCalculationServiceBlocking,
            ExpensesRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking,
            SearchIndexServiceGrpc.SearchIndexServiceBlockingStub searchSearchIndexServiceBlocking) {
        this.assetsDisposalCalculationServiceBlocking = assetsDisposalCalculationServiceBlocking;
        this.assetsFixedAssetServiceAsync = assetsFixedAssetServiceAsync;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.auditRetentionPolicyValidationServiceBlocking = auditRetentionPolicyValidationServiceBlocking;
        this.channels = channels;
        this.chatChatRoomServiceBlocking = chatChatRoomServiceBlocking;
        this.labelingLabelingTaskServiceBlocking = labelingLabelingTaskServiceBlocking;
        this.ledgerJournalEntryHistoryServiceBlocking = ledgerJournalEntryHistoryServiceBlocking;
        this.ledgerLedgerAccountServiceBlocking = ledgerLedgerAccountServiceBlocking;
        this.lendingRepaymentValidationServiceBlocking = lendingRepaymentValidationServiceBlocking;
        this.notificationsNotificationQueryServiceBlocking = notificationsNotificationQueryServiceBlocking;
        this.prescriptionsPrescriptionCalculationServiceBlocking = prescriptionsPrescriptionCalculationServiceBlocking;
        this.procurementPurchaseOrderValidationServiceBlocking = procurementPurchaseOrderValidationServiceBlocking;
        this.procurementRfqCalculationServiceBlocking = procurementRfqCalculationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.searchSearchIndexQueryServiceBlocking = searchSearchIndexQueryServiceBlocking;
        this.searchSearchIndexServiceBlocking = searchSearchIndexServiceBlocking;
    }

    /**
     * Calculates an expense report.
     */
    public CalculateExpenseReportResponse calculateExpenseReport(CalculateExpenseReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateExpenseReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpenseReport(request.getContext().getTenantId(), request.getExpenseReport());

        var outbound0 = CloneLedgerAccountRequest.newBuilder()
                .setContext(request.getContext())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getExpenseReport().getAudit().getCreatedBy())
                                .setUpdatedBy(request.getExpenseReport().getAudit().getUpdatedBy())
                                .build())
                        .setCard(LedgerAccountCardFunding.newBuilder()
                                .setLast4(switch (request.getExpenseReport().getSettlementCase()) {
                                    case CARD -> request.getExpenseReport().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getExpenseReport().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = ledgerLedgerAccountServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneLedgerAccount(outbound0);

        var outbound1 = CheckRepaymentEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setRepayment(Repayment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getExpenseReport().getAudit().getUpdatedBy())
                                .build())
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setName(reply0.getLedgerAccount().getContact().getName())
                                        .build())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.lending.v1.RepaymentValidationService");
        var serviceName1 = "acme.lending.v1" + "." + "RepaymentValidationService";
        var methodName1 = "Che" + "ckRepaymentEligibility";
        var descriptor1 = MethodDescriptor.<CheckRepaymentEligibilityRequest, CheckRepaymentEligibilityResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(CheckRepaymentEligibilityRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(CheckRepaymentEligibilityResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);

        var outbound2 = BatchCreateFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateFixedAssetsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(BatchCreateFixedAssetsResponse.getDefaultInstance());
        var input2 = assetsFixedAssetServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateFixedAssets(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getExpenseReport().getAudit().getCreatedAt())
                                .setCreatedBy(request.getExpenseReport().getAudit().getCreatedBy())
                                .setEtag(request.getExpenseReport().getAudit().getEtag())
                                .setUpdatedAt(request.getExpenseReport().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input3 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getExpenseReport().getExpenseReportId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateExpenseReport", request.getExpenseReport().getExpenseReportId());
        return CalculateExpenseReportResponse.newBuilder()
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getRepayment().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply1.getRepayment().getOrganizer().getEmail())
                                .build())
                        .setExpenseReportId(request.getExpenseReportId())
                        .setNotes(request.getExpenseReport().getNotes())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Estimates an expense report.
     */
    public EstimateExpenseReportResponse estimateExpenseReport(EstimateExpenseReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateExpenseReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpenseReport(request.getContext().getTenantId(), request.getExpenseReport());

        var futures0 = request.getExpenseReport().getLinesList().stream()
                .map(element -> NormalizeRetentionPolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getExpenseReport().getAudit().getUpdatedAt())
                                .build())
                        .setDescription(element.getDescription())
                        .setDiscount(RetentionPolicyDiscountAction.newBuilder()
                                .setAmount(element.getTax().getAmount())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> auditRetentionPolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeRetentionPolicy(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? NormalizeRetentionPolicyResponse.getDefaultInstance()
                : results0.getLast();
        log.atDebug().addKeyValue("auditValue", request.getExpenseReport().getExpenseReportId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateExpenseReport", request.getExpenseReport().getExpenseReportId());
        return EstimateExpenseReportResponse.newBuilder()
                .setBreakdown(EstimateExpenseReportResponse.Breakdown.newBuilder()
                        .setTotal(request.getExpenseReport().getTotal())
                        .build())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setExpenseReportId(request.getExpenseReportId())
                        .build())
                .build();
    }

    /**
     * Quotes an expense report.
     */
    public QuoteExpenseReportResponse quoteExpenseReport(QuoteExpenseReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuoteExpenseReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpenseReport(request.getContext().getTenantId(), request.getExpenseReport());

        var outbound0 = BatchGetLabelingTasksRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> labelingLabelingTaskServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetLabelingTasks(outbound0), rpcExecutor);

        var outbound1 = ExplainDisposalCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDisposal(Disposal.newBuilder()
                        .addComments(DisposalComment.newBuilder()
                                .setCreatedAt(request.getExpenseReport().getAudit().getCreatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getExpenseReport().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> assetsDisposalCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainDisposalCalculation(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getExpenseReport().getExpenseReportId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuoteExpenseReport", request.getExpenseReport().getExpenseReportId());
        return QuoteExpenseReportResponse.newBuilder()
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getDisposal().getAudit().getCreatedAt())
                                .setCreatedBy(reply1.getDisposal().getAudit().getCreatedBy())
                                .build())
                        .setExpenseReportId(request.getExpenseReportId())
                        .putAllMetadata(request.getExpenseReport().getMetadataMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Compares an expense report.
     */
    public CompareExpenseReportOptionsResponse compareExpenseReportOptions(CompareExpenseReportOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareExpenseReportOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpenseReport(request.getContext().getTenantId(), request.getExpenseReport());

        var outbound0 = ComparePrescriptionOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ComparePrescriptionOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(request.getExpenseReport().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .setPrescription(Prescription.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getExpenseReport().getBillingAddress().getCountryCode())
                                .build())
                        .setCard(PrescriptionCardSettlement.newBuilder()
                                .setHolderName(switch (request.getExpenseReport().getSettlementCase()) {
                                    case CARD -> request.getExpenseReport().getCard().getHolderName();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(PatientRef.newBuilder()
                                .setEmail(request.getExpenseReport().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = prescriptionsPrescriptionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).comparePrescriptionOptions(outbound0);

        var outbound1 = ScoreRepaymentRequest.newBuilder()
                .setContext(request.getContext())
                .setRepayment(Repayment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getPrescription().getAudit().getCreatedAt())
                                .build())
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getExpenseReport().getCounterparty().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.lending.v1.RepaymentValidationService");
        var selected1 = RepaymentValidationServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.scoreRepayment(outbound1);

        var outbound2 = SuggestSearchIndexesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestSearchIndexesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getPrescription().getAudit().getCreatedAt())
                        .setOwnerId(request.getExpenseReport().getCounterparty().getCustomerId())
                        .setRegion(reply0.getPrescription().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply2 = RpcSupport.call(outbound2, value -> searchSearchIndexQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestSearchIndexes(value));

        var outbound3 = JsonCodec.roundTrip(BatchGetSearchIndexesRequest.newBuilder()
                .setContext(request.getContext())
                .build(), BatchGetSearchIndexesRequest.class);
        var reply3 = searchSearchIndexServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetSearchIndexes(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getExpenseReport().getExpenseReportId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareExpenseReportOptions", request.getExpenseReport().getExpenseReportId());
        return CompareExpenseReportOptionsResponse.newBuilder()
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getRepayment().getAudit().getEtag())
                                .build())
                        .setBillingAddress(request.getExpenseReport().getBillingAddress())
                        .setDueAt(reply0.getPrescription().getDueAt())
                        .setExpenseReportId(request.getExpenseReportId())
                        .setTotal(reply1.getBreakdown().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply3.getResult().getCorrelationId())
                        .setMessage(reply3.getResult().getMessage())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Recalculates an expense report.
     */
    public RecalculateExpenseReportsResponse recalculateExpenseReports(RecalculateExpenseReportsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateExpenseReports")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpenseReport(request.getContext().getTenantId(), request.getExpenseReport());

        var outbound0 = RpcSupport.unpack(Any.pack(EstimateRfqRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EstimateRfqRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getDetails().getDestination().getLinesList())
                                .setPostalCode(request.getExpenseReport().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .setRfq(Rfq.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getExpenseReport().getAudit().getUpdatedAt())
                                .setUpdatedBy(request.getExpenseReport().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build()), EstimateRfqRequest.class);
        var reply0 = procurementRfqCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateRfq(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getExpenseReport().getExpenseReportId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateExpenseReports", request.getExpenseReport().getExpenseReportId());
        return RecalculateExpenseReportsResponse.newBuilder()
                .setBreakdown(RecalculateExpenseReportsResponse.Breakdown.newBuilder()
                        .setTotal(reply0.getBreakdown().getTotal())
                        .build())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setExpenseReportId(request.getExpenseReportId())
                        .setNotes(request.getExpenseReport().getNotes())
                        .build())
                .build();
    }

    /**
     * Simulates an expense report.
     */
    public SimulateExpenseReportResponse simulateExpenseReport(SimulateExpenseReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateExpenseReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpenseReport(request.getContext().getTenantId(), request.getExpenseReport());

        var outbound0 = CreateChatRoomRequest.newBuilder()
                .setChatRoom(ChatRoom.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getExpenseReport().getBillingAddress().getPostalCode())
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> chatChatRoomServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createChatRoom(outbound0), rpcExecutor);

        var outbound1 = ResolveNotificationsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> notificationsNotificationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveNotifications(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getExpenseReport().getExpenseReportId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateExpenseReport", request.getExpenseReport().getExpenseReportId());
        return SimulateExpenseReportResponse.newBuilder()
                .setBreakdown(SimulateExpenseReportResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .build())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply0.getChatRoom().getOwner().getDisplayName())
                                .build())
                        .setExpenseReportId(request.getExpenseReportId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Explains an expense report.
     */
    public ExplainExpenseReportCalculationResponse explainExpenseReportCalculation(ExplainExpenseReportCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainExpenseReportCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpenseReport(request.getContext().getTenantId(), request.getExpenseReport());

        var outbound0 = CountNotificationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountNotificationsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getExpenseReport().getDueAt())
                        .setRegion(request.getExpenseReport().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> notificationsNotificationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countNotifications(value)).orElseThrow();

        var outbound1 = LintPurchaseOrdersRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getExpenseReport().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getExpenseReport().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .build();
        RpcPort<LintPurchaseOrdersRequest, LintPurchaseOrdersResponse> port1 = RpcPort.adapt(procurementPurchaseOrderValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::lintPurchaseOrders);
        var reply1 = port1.exchange(outbound1);

        var futures2 = request.getExpenseReport().getAttachmentsList().stream()
                .map(element -> CompareJournalEntryVersionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CompareJournalEntryVersionsRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setJournalEntry(JournalEntry.newBuilder()
                        .addAttachments(JournalEntryAttachment.newBuilder()
                                .setChecksum(element.getChecksum())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> ledgerJournalEntryHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareJournalEntryVersions(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures2.toArray(CompletableFuture[]::new)).join();
        var results2 = futures2.stream().map(CompletableFuture::join).toList();
        var reply2 = results2.isEmpty() ? CompareJournalEntryVersionsResponse.getDefaultInstance()
                : results2.getLast();

        var outbound3 = PreviewRepaymentRequest.newBuilder()
                .setContext(request.getContext())
                .setRepayment(Repayment.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setLocality(request.getExpenseReport().getBillingAddress().getLocality())
                                .setRecipient(request.getExpenseReport().getBillingAddress().getRecipient())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setDisplayName(reply2.getJournalEntry().getCounterparty().getDisplayName())
                                .build())
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setPhone(request.getDetails().getContact().getPhone())
                                        .build())
                                .build())
                        .build())
                .build();
        Function<PreviewRepaymentRequest, PreviewRepaymentResponse> operation3 = lendingRepaymentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::previewRepayment;
        var reply3 = operation3.apply(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getExpenseReport().getExpenseReportId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainExpenseReportCalculation", request.getExpenseReport().getExpenseReportId());
        return ExplainExpenseReportCalculationResponse.newBuilder()
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply3.getRepayment().getAudit().getEtag())
                                .build())
                        .setExpenseReportId(request.getExpenseReportId())
                        .putAllMetadata(request.getExpenseReport().getMetadataMap())
                        .build())
                .setOutcome(ExplainExpenseReportCalculationResponse.Outcome.newBuilder()
                        .setReferenceId(reply1.getOutcome().getReferenceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Returns an expense report.
     */
    public GetExpenseReportRatesResponse getExpenseReportRates(GetExpenseReportRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetExpenseReportRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ExpenseReport", request.getExpenseReportId(), "GetExpenseReportRates");

        var outbound0 = ExplainRepaymentDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setRepayment(Repayment.newBuilder()
                        .setOrganizer(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(lendingRepaymentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainRepaymentDecision(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = CheckRepaymentEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setRepayment(Repayment.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setLocality(reply0.getRepayment().getLocation().getLocality())
                                .build())
                        .setWeekly(RepaymentWeeklyRule.newBuilder()
                                .setInterval(reply0.getRepayment().getDaily().getInterval())
                                .build())
                        .build())
                .build();
        var reply1 = lendingRepaymentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkRepaymentEligibility(outbound1);
        return GetExpenseReportRatesResponse.newBuilder()
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getRepayment().getAudit().getUpdatedBy())
                                .build())
                        .setExpenseReportId(request.getExpenseReportId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }
}
