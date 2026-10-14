package com.acme.finance.domains.financeportal;

import com.acme.audit.v1.AuditTrailNotificationServiceGrpc;
import com.acme.audit.v1.SubscribeAuditTrailUpdatesRequest;
import com.acme.audit.v1.SubscribeAuditTrailUpdatesResponse;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.CreateAccessPolicyRequest;
import com.acme.authorization.v1.CreateAccessPolicyResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CommentHouseholdApprovalRequest;
import com.acme.customers.v1.CommentHouseholdApprovalResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.Household;
import com.acme.driverapp.v1.CreateDriverHomeRequest;
import com.acme.driverapp.v1.CreateDriverHomeResponse;
import com.acme.driverapp.v1.DriverHome;
import com.acme.driverapp.v1.DriverHomeServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.financeportal.v1.AdvanceCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.AdvanceCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.ApprovalInbox;
import com.acme.financeportal.v1.ApprovalInboxAttachment;
import com.acme.financeportal.v1.ApprovalInboxBankSettlement;
import com.acme.financeportal.v1.ApprovalInboxLine;
import com.acme.financeportal.v1.ApprovalInboxLineTax;
import com.acme.financeportal.v1.ApprovalInboxWorkflowServiceGrpc;
import com.acme.financeportal.v1.CancelApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.CancelApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.CancelCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.CancelCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.CloseChecklist;
import com.acme.financeportal.v1.InspectCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.InspectCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.ListApprovalInboxWorkflowsRequest;
import com.acme.financeportal.v1.ListApprovalInboxWorkflowsResponse;
import com.acme.financeportal.v1.ListCloseChecklistWorkflowsRequest;
import com.acme.financeportal.v1.ListCloseChecklistWorkflowsResponse;
import com.acme.financeportal.v1.PauseApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.PauseApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.PauseCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.PauseCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.ResumeApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.ResumeApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.ResumeCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.ResumeCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.RetryApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.RetryApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.RetryCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.RetryCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.SignalApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.SignalApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.SignalCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.SignalCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.StartCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.StartCloseChecklistWorkflowResponse;
import com.acme.identity.v1.FindUsersRequest;
import com.acme.identity.v1.FindUsersResponse;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.RejectClaimRequest;
import com.acme.insurance.v1.RejectClaimResponse;
import com.acme.ledger.v1.CompareFiscalPeriodVersionsRequest;
import com.acme.ledger.v1.CompareFiscalPeriodVersionsResponse;
import com.acme.ledger.v1.ExportJournalEntriesRequest;
import com.acme.ledger.v1.ExportJournalEntriesResponse;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.FiscalPeriodHistoryServiceGrpc;
import com.acme.ledger.v1.FiscalPeriodParticipant;
import com.acme.ledger.v1.JournalEntryQueryServiceGrpc;
import com.acme.metrics.v1.CountMetricSeriesesRequest;
import com.acme.metrics.v1.CountMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeriesQueryServiceGrpc;
import com.acme.suppliers.v1.SubmitSupplierContractRequest;
import com.acme.suppliers.v1.SubmitSupplierContractResponse;
import com.acme.suppliers.v1.SupplierContract;
import com.acme.suppliers.v1.SupplierContractApprovalServiceGrpc;
import com.acme.suppliers.v1.SupplierContractLine;
import com.acme.suppliers.v1.SupplierContractLineTax;
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
 * Runs the long-lived workflow that takes a close checklist from start to finish. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class CloseChecklistWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(CloseChecklistWorkflowApplication.class);

    private final AuditTrailNotificationServiceGrpc.AuditTrailNotificationServiceBlockingStub auditAuditTrailNotificationServiceBlocking;

    private final AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking;

    private final ChannelRegistry channels;

    private final DriverHomeServiceGrpc.DriverHomeServiceBlockingStub driverappDriverHomeServiceBlocking;

    private final ApprovalInboxWorkflowServiceGrpc.ApprovalInboxWorkflowServiceBlockingStub financeportalApprovalInboxWorkflowServiceBlocking;

    private final FiscalPeriodHistoryServiceGrpc.FiscalPeriodHistoryServiceBlockingStub ledgerFiscalPeriodHistoryServiceBlocking;

    private final JournalEntryQueryServiceGrpc.JournalEntryQueryServiceBlockingStub ledgerJournalEntryQueryServiceBlocking;

    private final CloseChecklistWorkflowRequestMapper mapper;

    private final MetricSeriesQueryServiceGrpc.MetricSeriesQueryServiceBlockingStub metricsMetricSeriesQueryServiceBlocking;

    private final FinanceportalRepository repository;

    private final Executor rpcExecutor;

    private final SupplierContractApprovalServiceGrpc.SupplierContractApprovalServiceBlockingStub suppliersSupplierContractApprovalServiceBlocking;

    public CloseChecklistWorkflowApplication(
            AuditTrailNotificationServiceGrpc.AuditTrailNotificationServiceBlockingStub auditAuditTrailNotificationServiceBlocking,
            AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking,
            ChannelRegistry channels,
            DriverHomeServiceGrpc.DriverHomeServiceBlockingStub driverappDriverHomeServiceBlocking,
            ApprovalInboxWorkflowServiceGrpc.ApprovalInboxWorkflowServiceBlockingStub financeportalApprovalInboxWorkflowServiceBlocking,
            FiscalPeriodHistoryServiceGrpc.FiscalPeriodHistoryServiceBlockingStub ledgerFiscalPeriodHistoryServiceBlocking,
            JournalEntryQueryServiceGrpc.JournalEntryQueryServiceBlockingStub ledgerJournalEntryQueryServiceBlocking,
            CloseChecklistWorkflowRequestMapper mapper,
            MetricSeriesQueryServiceGrpc.MetricSeriesQueryServiceBlockingStub metricsMetricSeriesQueryServiceBlocking,
            FinanceportalRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierContractApprovalServiceGrpc.SupplierContractApprovalServiceBlockingStub suppliersSupplierContractApprovalServiceBlocking) {
        this.auditAuditTrailNotificationServiceBlocking = auditAuditTrailNotificationServiceBlocking;
        this.authorizationAccessPolicyServiceBlocking = authorizationAccessPolicyServiceBlocking;
        this.channels = channels;
        this.driverappDriverHomeServiceBlocking = driverappDriverHomeServiceBlocking;
        this.financeportalApprovalInboxWorkflowServiceBlocking = financeportalApprovalInboxWorkflowServiceBlocking;
        this.ledgerFiscalPeriodHistoryServiceBlocking = ledgerFiscalPeriodHistoryServiceBlocking;
        this.ledgerJournalEntryQueryServiceBlocking = ledgerJournalEntryQueryServiceBlocking;
        this.mapper = mapper;
        this.metricsMetricSeriesQueryServiceBlocking = metricsMetricSeriesQueryServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierContractApprovalServiceBlocking = suppliersSupplierContractApprovalServiceBlocking;
    }

    /**
     * Starts a close checklist.
     */
    public StartCloseChecklistWorkflowResponse startCloseChecklistWorkflow(StartCloseChecklistWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartCloseChecklistWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCloseChecklist(request.getContext().getTenantId(), request.getCloseChecklist());

        var outbound0 = BeanCopies.copy(CreateDriverHomeRequest.newBuilder()
                .setContext(request.getContext())
                .setDriverHome(DriverHome.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getCloseChecklist().getCounterparty().getDisplayName())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getCloseChecklist().getAudit().getCreatedBy())
                                .setUpdatedBy(request.getCloseChecklist().getAudit().getUpdatedBy())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .build())
                        .build())
                .build(), CreateDriverHomeRequest.class);
        var reply0 = driverappDriverHomeServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createDriverHome(outbound0);

        var outbound1 = FindUsersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindUsersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getCloseChecklist().getAudit().getUpdatedAt())
                        .putAllLabels(request.getDetails().getParametersMap())
                        .setOwnerId(reply0.getDriverHome().getAssignee().getUserId())
                        .setRegion(request.getCloseChecklist().getBillingAddress().getRegion())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.identity.v1.UserQueryService");
        var serviceName1 = "acme.identity.v1" + "." + "UserQueryService";
        var methodName1 = "Fin" + "dUsers";
        var descriptor1 = MethodDescriptor.<FindUsersRequest, FindUsersResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(FindUsersRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(FindUsersResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);
        log.atDebug().addKeyValue("auditValue", request.getCloseChecklist().getCloseChecklistId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartCloseChecklistWorkflow", request.getCloseChecklist().getCloseChecklistId());
        return StartCloseChecklistWorkflowResponse.newBuilder()
                .setCloseChecklist(CloseChecklist.newBuilder()
                        .setCloseChecklistId(request.getCloseChecklistId())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply0.getDriverHome().getAssignee().getDisplayName())
                                .build())
                        .setDueAt(request.getCloseChecklist().getDueAt())
                        .build())
                .setOutcome(StartCloseChecklistWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Advances a close checklist.
     */
    public AdvanceCloseChecklistWorkflowResponse advanceCloseChecklistWorkflow(AdvanceCloseChecklistWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceCloseChecklistWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCloseChecklist(request.getContext().getTenantId(), request.getCloseChecklist());

        var outbound0 = ExportJournalEntriesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportJournalEntriesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getCloseChecklist().getAudit().getCreatedAt())
                        .putAllLabels(request.getDetails().getParametersMap())
                        .setOwnerId(request.getCloseChecklist().getCloseChecklistId())
                        .setRegion(request.getCloseChecklist().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream0 = ledgerJournalEntryQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportJournalEntries(outbound0);
        var reply0 = ExportJournalEntriesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = RejectClaimRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getCloseChecklist().getAudit().getCreatedBy())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotify().getName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel1 = channels.lookup("acme.insurance.v1.ClaimApprovalService");
        var serviceName1 = "acme.insurance.v1" + "." + "ClaimApprovalService";
        var methodName1 = "Rej" + "ectClaim";
        var descriptor1 = MethodDescriptor.<RejectClaimRequest, RejectClaimResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(RejectClaimRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(RejectClaimResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);
        log.atDebug().addKeyValue("auditValue", request.getCloseChecklist().getCounterparty().getCustomerId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceCloseChecklistWorkflow", request.getCloseChecklist().getCounterparty().getCustomerId());
        return AdvanceCloseChecklistWorkflowResponse.newBuilder()
                .setCloseChecklist(CloseChecklist.newBuilder()
                        .setCloseChecklistId(request.getCloseChecklistId())
                        .setDueAt(reply1.getClaim().getSla().getDueAt())
                        .setNumber(request.getCloseChecklist().getNumber())
                        .build())
                .build();
    }

    /**
     * Cancels a close checklist.
     */
    public CancelCloseChecklistWorkflowResponse cancelCloseChecklistWorkflow(CancelCloseChecklistWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelCloseChecklistWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCloseChecklist(request.getContext().getTenantId(), request.getCloseChecklist());

        var outbound0 = mapper.cancelCloseChecklistWorkflowStage0(request);
        var reply0 = ledgerFiscalPeriodHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareFiscalPeriodVersions(outbound0);

        var outbound1 = RetryApprovalInboxWorkflowRequest.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getCloseChecklist().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getFiscalPeriod().getLocation().getLinesList())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RetryApprovalInboxWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<RetryApprovalInboxWorkflowRequest, RetryApprovalInboxWorkflowResponse>() {
            @Override
            public RetryApprovalInboxWorkflowResponse apply(RetryApprovalInboxWorkflowRequest input) {
                return financeportalApprovalInboxWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryApprovalInboxWorkflow(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);

        var outbound2 = SubscribeAuditTrailUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubscribeAuditTrailUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(reply0.getFiscalPeriod().getOrganizer().getEmail())
                                .setName(request.getDetails().getNotify().getName())
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply2 = auditAuditTrailNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).subscribeAuditTrailUpdates(outbound2);

        var outbound3 = SignalApprovalInboxWorkflowRequest.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setBank(ApprovalInboxBankSettlement.newBuilder()
                                .setBic(switch (request.getCloseChecklist().getSettlementCase()) {
                                    case BANK -> request.getCloseChecklist().getBank().getBic();
                                    default -> "";
                                })
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getCloseChecklist().getBillingAddress().getLinesList())
                                .setRecipient(request.getCloseChecklist().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply3 = financeportalApprovalInboxWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).signalApprovalInboxWorkflow(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getCloseChecklist().getCloseChecklistId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelCloseChecklistWorkflow", request.getCloseChecklist().getCloseChecklistId());
        return CancelCloseChecklistWorkflowResponse.newBuilder()
                .setCloseChecklist(CloseChecklist.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getFiscalPeriod().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply3.getApprovalInbox().getBillingAddress().getLocality())
                                .setPostalCode(reply1.getApprovalInbox().getBillingAddress().getPostalCode())
                                .build())
                        .setCloseChecklistId(request.getCloseChecklistId())
                        .putAllMetadata(reply2.getAuditTrail().getMetadataMap())
                        .setNotes(request.getCloseChecklist().getNotes())
                        .build())
                .setOutcome(CancelCloseChecklistWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Retrys a close checklist.
     */
    public RetryCloseChecklistWorkflowResponse retryCloseChecklistWorkflow(RetryCloseChecklistWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryCloseChecklistWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCloseChecklist(request.getContext().getTenantId(), request.getCloseChecklist());

        var results0 = new ArrayList<RetryApprovalInboxWorkflowResponse>();
        for (var element : request.getCloseChecklist().getAttachmentsList()) {
            RetryApprovalInboxWorkflowRequest itemRequest = RetryApprovalInboxWorkflowRequest.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .addAttachments(ApprovalInboxAttachment.newBuilder()
                                .setContentType(element.getContentType())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
            results0.add(financeportalApprovalInboxWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryApprovalInboxWorkflow(itemRequest));
        }
        var reply0 = results0.isEmpty() ? RetryApprovalInboxWorkflowResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = SignalApprovalInboxWorkflowRequest.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setBank(ApprovalInboxBankSettlement.newBuilder()
                                .setIban(switch (reply0.getApprovalInbox().getSettlementCase()) {
                                    case BANK -> reply0.getApprovalInbox().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = financeportalApprovalInboxWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).signalApprovalInboxWorkflow(outbound1);

        var futures2 = request.getCloseChecklist().getLinesList().stream()
                .map(element -> CancelApprovalInboxWorkflowRequest.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .addLines(ApprovalInboxLine.newBuilder()
                                .putAllAttributes(element.getAttributesMap())
                                .setDescription(element.getDescription())
                                .setTax(ApprovalInboxLineTax.newBuilder()
                                        .setJurisdiction(element.getTax().getJurisdiction())
                                        .build())
                                .setUnitPrice(element.getUnitPrice())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> financeportalApprovalInboxWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cancelApprovalInboxWorkflow(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures2.toArray(CompletableFuture[]::new)).join();
        var results2 = futures2.stream().map(CompletableFuture::join).toList();
        var reply2 = results2.isEmpty() ? CancelApprovalInboxWorkflowResponse.getDefaultInstance()
                : results2.getLast();

        var results3 = new ArrayList<SubmitSupplierContractResponse>();
        for (var element : request.getCloseChecklist().getLinesList()) {
            SubmitSupplierContractRequest itemRequest = SubmitSupplierContractRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierContract(SupplierContract.newBuilder()
                        .addLines(SupplierContractLine.newBuilder()
                                .setTax(SupplierContractLineTax.newBuilder()
                                        .setRate(element.getTax().getRate())
                                        .build())
                                .build())
                        .build())
                .build();
            results3.add(suppliersSupplierContractApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitSupplierContract(itemRequest));
        }
        var reply3 = results3.isEmpty() ? SubmitSupplierContractResponse.getDefaultInstance()
                : results3.getLast();
        log.atDebug().addKeyValue("auditValue", request.getCloseChecklist().getCloseChecklistId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryCloseChecklistWorkflow", request.getCloseChecklist().getCloseChecklistId());
        return RetryCloseChecklistWorkflowResponse.newBuilder()
                .setCloseChecklist(CloseChecklist.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply1.getApprovalInbox().getBillingAddress().getRegion())
                                .build())
                        .setCloseChecklistId(request.getCloseChecklistId())
                        .setIssuedAt(reply1.getApprovalInbox().getIssuedAt())
                        .setNotes(request.getCloseChecklist().getNotes())
                        .build())
                .build();
    }

    /**
     * Pauses a close checklist.
     */
    public PauseCloseChecklistWorkflowResponse pauseCloseChecklistWorkflow(PauseCloseChecklistWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseCloseChecklistWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCloseChecklist(request.getContext().getTenantId(), request.getCloseChecklist());

        var outbound0 = ResumeApprovalInboxWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ResumeApprovalInboxWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<ResumeApprovalInboxWorkflowRequest, ResumeApprovalInboxWorkflowResponse>() {
            @Override
            public ResumeApprovalInboxWorkflowResponse apply(ResumeApprovalInboxWorkflowRequest input) {
                return financeportalApprovalInboxWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resumeApprovalInboxWorkflow(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = ListApprovalInboxWorkflowsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListApprovalInboxWorkflowsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getCloseChecklist().getAudit().getUpdatedAt())
                        .setOwnerId(request.getCloseChecklist().getCounterparty().getCustomerId())
                        .setRegion(request.getCloseChecklist().getBillingAddress().getRegion())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.financeportal.v1.ApprovalInboxWorkflowService");
        var serviceName1 = "acme.financeportal.v1" + "." + "ApprovalInboxWorkflowService";
        var methodName1 = "Lis" + "tApprovalInboxWorkflows";
        var descriptor1 = MethodDescriptor.<ListApprovalInboxWorkflowsRequest, ListApprovalInboxWorkflowsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(ListApprovalInboxWorkflowsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ListApprovalInboxWorkflowsResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);
        log.atDebug().addKeyValue("auditValue", request.getCloseChecklist().getCloseChecklistId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseCloseChecklistWorkflow", request.getCloseChecklist().getCloseChecklistId());
        return PauseCloseChecklistWorkflowResponse.newBuilder()
                .setCloseChecklist(CloseChecklist.newBuilder()
                        .setBillingAddress(reply0.getApprovalInbox().getBillingAddress())
                        .setCloseChecklistId(request.getCloseChecklistId())
                        .setDueAt(request.getCloseChecklist().getDueAt())
                        .build())
                .setOutcome(PauseCloseChecklistWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Resumes a close checklist.
     */
    public ResumeCloseChecklistWorkflowResponse resumeCloseChecklistWorkflow(ResumeCloseChecklistWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeCloseChecklistWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCloseChecklist(request.getContext().getTenantId(), request.getCloseChecklist());

        var results0 = new ArrayList<PauseApprovalInboxWorkflowResponse>();
        for (var element : request.getCloseChecklist().getLinesList()) {
            PauseApprovalInboxWorkflowRequest itemRequest = PauseApprovalInboxWorkflowRequest.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .addLines(ApprovalInboxLine.newBuilder()
                                .setAmount(element.getTax().getAmount())
                                .setDescription(element.getDescription())
                                .setSku(element.getSku())
                                .setTax(ApprovalInboxLineTax.newBuilder()
                                        .setRate(element.getTax().getRate())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
            results0.add(financeportalApprovalInboxWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pauseApprovalInboxWorkflow(itemRequest));
        }
        var reply0 = results0.isEmpty() ? PauseApprovalInboxWorkflowResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = CreateAccessPolicyRequest.newBuilder()
                .setAccessPolicy(AccessPolicy.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getApprovalInbox().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = authorizationAccessPolicyServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createAccessPolicy(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getCloseChecklist().getCloseChecklistId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeCloseChecklistWorkflow", request.getCloseChecklist().getCloseChecklistId());
        return ResumeCloseChecklistWorkflowResponse.newBuilder()
                .setCloseChecklist(CloseChecklist.newBuilder()
                        .setCloseChecklistId(request.getCloseChecklistId())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply1.getAccessPolicy().getOwner().getDisplayName())
                                .build())
                        .setIssuedAt(request.getCloseChecklist().getIssuedAt())
                        .build())
                .build();
    }

    /**
     * Returns a close checklist.
     */
    public InspectCloseChecklistWorkflowResponse inspectCloseChecklistWorkflow(InspectCloseChecklistWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectCloseChecklistWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CloseChecklist", request.getCloseChecklistId(), "InspectCloseChecklistWorkflow");
        return InspectCloseChecklistWorkflowResponse.newBuilder()
                .setCloseChecklist(CloseChecklist.newBuilder()
                        .setCloseChecklistId(request.getCloseChecklistId())
                        .build())
                .build();
    }

    /**
     * Lists close checklists a page at a time.
     */
    public ListCloseChecklistWorkflowsResponse listCloseChecklistWorkflows(ListCloseChecklistWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListCloseChecklistWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CloseChecklist", request.getContext().getTraceId(), "ListCloseChecklistWorkflows");

        var outbound0 = CommentHouseholdApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .setRegion(request.getFilter().getRegion())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(request.getContext().getActor().getEmail())
                                                .setUserId(request.getContext().getActor().getUserId())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.customers.v1.HouseholdApprovalService");
        var serviceName0 = "acme.customers.v1" + "." + "HouseholdApprovalService";
        var methodName0 = "Com" + "mentHouseholdApproval";
        var descriptor0 = MethodDescriptor.<CommentHouseholdApprovalRequest, CommentHouseholdApprovalResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(CommentHouseholdApprovalRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(CommentHouseholdApprovalResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListCloseChecklistWorkflows", request.getPage().getPageToken());
        return ListCloseChecklistWorkflowsResponse.newBuilder()
                .addCloseChecklists(CloseChecklist.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getHousehold().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
    }

    /**
     * Signals a close checklist.
     */
    public SignalCloseChecklistWorkflowResponse signalCloseChecklistWorkflow(SignalCloseChecklistWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalCloseChecklistWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCloseChecklist(request.getContext().getTenantId(), request.getCloseChecklist());

        var outbound0 = CountMetricSeriesesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountMetricSeriesesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getCloseChecklist().getAudit().getUpdatedAt())
                        .setRegion(request.getCloseChecklist().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(metricsMetricSeriesQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countMetricSerieses(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = RetryApprovalInboxWorkflowRequest.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getCloseChecklist().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getCloseChecklist().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RetryApprovalInboxWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(financeportalApprovalInboxWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryApprovalInboxWorkflow(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound2 = BeanCopies.copy(ResumeApprovalInboxWorkflowRequest.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getCloseChecklist().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build(), ResumeApprovalInboxWorkflowRequest.class);
        var reply2 = financeportalApprovalInboxWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resumeApprovalInboxWorkflow(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getCloseChecklist().getCloseChecklistId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalCloseChecklistWorkflow", request.getCloseChecklist().getCloseChecklistId());
        return SignalCloseChecklistWorkflowResponse.newBuilder()
                .setCloseChecklist(CloseChecklist.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply2.getApprovalInbox().getBillingAddress().getRegion())
                                .build())
                        .setCloseChecklistId(request.getCloseChecklistId())
                        .setNotes(request.getCloseChecklist().getNotes())
                        .build())
                .setOutcome(SignalCloseChecklistWorkflowResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply1.getOutcome().getEffectiveAt())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }
}
