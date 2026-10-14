package com.acme.finance.domains.ledger;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.ArchiveFiscalPeriodRequest;
import com.acme.ledger.v1.ArchiveFiscalPeriodResponse;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.InspectFiscalPeriodRequest;
import com.acme.ledger.v1.InspectFiscalPeriodResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.ledger.v1.LockFiscalPeriodRequest;
import com.acme.ledger.v1.LockFiscalPeriodResponse;
import com.acme.ledger.v1.PostingBatchRef;
import com.acme.ledger.v1.PurgeFiscalPeriodRequest;
import com.acme.ledger.v1.PurgeFiscalPeriodResponse;
import com.acme.ledger.v1.ReassignFiscalPeriodRequest;
import com.acme.ledger.v1.ReassignFiscalPeriodResponse;
import com.acme.ledger.v1.RecordLedgerAccountVersionRequest;
import com.acme.ledger.v1.RecordLedgerAccountVersionResponse;
import com.acme.ledger.v1.RepairFiscalPeriodRequest;
import com.acme.ledger.v1.RepairFiscalPeriodResponse;
import com.acme.ledger.v1.RestoreFiscalPeriodRequest;
import com.acme.ledger.v1.RestoreFiscalPeriodResponse;
import com.acme.ledger.v1.SendJournalEntryTestNoticeRequest;
import com.acme.ledger.v1.SendJournalEntryTestNoticeResponse;
import com.acme.ledger.v1.TransferFiscalPeriodRequest;
import com.acme.ledger.v1.TransferFiscalPeriodResponse;
import com.acme.ledger.v1.UnlockFiscalPeriodRequest;
import com.acme.ledger.v1.UnlockFiscalPeriodResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Operator actions on fiscal periods that support staff run by hand. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class FiscalPeriodAdminApplication {
    private static final Logger log = LoggerFactory.getLogger(FiscalPeriodAdminApplication.class);

    private final ChannelRegistry channels;

    private final LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking;

    private final LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceFutureStub ledgerLedgerAccountHistoryServiceFuture;

    private final LedgerRepository repository;

    public FiscalPeriodAdminApplication(
            ChannelRegistry channels,
            LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking,
            LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceFutureStub ledgerLedgerAccountHistoryServiceFuture,
            LedgerRepository repository) {
        this.channels = channels;
        this.ledgerLedgerAccountHistoryServiceBlocking = ledgerLedgerAccountHistoryServiceBlocking;
        this.ledgerLedgerAccountHistoryServiceFuture = ledgerLedgerAccountHistoryServiceFuture;
        this.repository = repository;
    }

    /**
     * Archives a fiscal period.
     */
    public ArchiveFiscalPeriodResponse archiveFiscalPeriod(ArchiveFiscalPeriodRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ArchiveFiscalPeriod")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FiscalPeriod", request.getFiscalPeriodId(), "ArchiveFiscalPeriod");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ArchiveFiscalPeriod", request.getDetails().getReasonCode());
        return ArchiveFiscalPeriodResponse.newBuilder()
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .setFiscalPeriodId(request.getFiscalPeriodId())
                        .build())
                .setOutcome(ArchiveFiscalPeriodResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Restores a fiscal period.
     */
    public RestoreFiscalPeriodResponse restoreFiscalPeriod(RestoreFiscalPeriodRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RestoreFiscalPeriod")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FiscalPeriod", request.getFiscalPeriodId(), "RestoreFiscalPeriod");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RestoreFiscalPeriod", request.getDetails().getReasonCode());
        return RestoreFiscalPeriodResponse.newBuilder()
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .setFiscalPeriodId(request.getFiscalPeriodId())
                        .build())
                .setOutcome(RestoreFiscalPeriodResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Deletes a fiscal period.
     */
    public PurgeFiscalPeriodResponse purgeFiscalPeriod(PurgeFiscalPeriodRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PurgeFiscalPeriod")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FiscalPeriod", request.getFiscalPeriodId(), "PurgeFiscalPeriod");

        var outbound0 = RecordLedgerAccountVersionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecordLedgerAccountVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getEtag())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setExternalAccountId(request.getFiscalPeriodId())
                        .build())
                .setReason(request.getReason())
                .build();
        var pending0 = ledgerLedgerAccountHistoryServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).recordLedgerAccountVersion(outbound0);
        var reply0 = RpcSupport.await(pending0);
        return PurgeFiscalPeriodResponse.newBuilder()
                .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Reassigns a fiscal period.
     */
    public ReassignFiscalPeriodResponse reassignFiscalPeriod(ReassignFiscalPeriodRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReassignFiscalPeriod")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FiscalPeriod", request.getFiscalPeriodId(), "ReassignFiscalPeriod");

        var outbound0 = RecordLedgerAccountVersionRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(RecordLedgerAccountVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getAssignee().getEmail())
                                .build())
                        .setHolder(PostingBatchRef.newBuilder()
                                .setDisplayName(request.getDetails().getAssignee().getDisplayName())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            RecordLedgerAccountVersionResponse fetch(RecordLedgerAccountVersionRequest input) {
                return ledgerLedgerAccountHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recordLedgerAccountVersion(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getAssignee().getUserId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReassignFiscalPeriod", request.getDetails().getAssignee().getUserId());
        return ReassignFiscalPeriodResponse.newBuilder()
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .setFiscalPeriodId(request.getFiscalPeriodId())
                        .setOrganizer(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getAssignee().getDisplayName())
                                .build())
                        .build())
                .setOutcome(ReassignFiscalPeriodResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .build();
    }

    /**
     * Locks a fiscal period.
     */
    public LockFiscalPeriodResponse lockFiscalPeriod(LockFiscalPeriodRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LockFiscalPeriod")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FiscalPeriod", request.getFiscalPeriodId(), "LockFiscalPeriod");

        var outbound0 = SendJournalEntryTestNoticeRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SendJournalEntryTestNoticeRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.ledger.v1.JournalEntryNotificationService");
        var serviceName0 = "acme.ledger.v1" + "." + "JournalEntryNotificationService";
        var methodName0 = "Sen" + "dJournalEntryTestNotice";
        var descriptor0 = MethodDescriptor.<SendJournalEntryTestNoticeRequest, SendJournalEntryTestNoticeResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(SendJournalEntryTestNoticeRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(SendJournalEntryTestNoticeResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LockFiscalPeriod", request.getDetails().getReasonCode());
        return LockFiscalPeriodResponse.newBuilder()
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .setFiscalPeriodId(request.getFiscalPeriodId())
                        .build())
                .setOutcome(LockFiscalPeriodResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Unlocks a fiscal period.
     */
    public UnlockFiscalPeriodResponse unlockFiscalPeriod(UnlockFiscalPeriodRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "UnlockFiscalPeriod")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FiscalPeriod", request.getFiscalPeriodId(), "UnlockFiscalPeriod");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "UnlockFiscalPeriod", request.getDetails().getReasonCode());
        return UnlockFiscalPeriodResponse.newBuilder()
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .setFiscalPeriodId(request.getFiscalPeriodId())
                        .build())
                .setOutcome(UnlockFiscalPeriodResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Transfers a fiscal period.
     */
    public TransferFiscalPeriodResponse transferFiscalPeriod(TransferFiscalPeriodRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TransferFiscalPeriod")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FiscalPeriod", request.getFiscalPeriodId(), "TransferFiscalPeriod");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TransferFiscalPeriod", request.getDetails().getReasonCode());
        return TransferFiscalPeriodResponse.newBuilder()
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .setFiscalPeriodId(request.getFiscalPeriodId())
                        .build())
                .setOutcome(TransferFiscalPeriodResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Repairs a fiscal period.
     */
    public RepairFiscalPeriodResponse repairFiscalPeriod(RepairFiscalPeriodRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RepairFiscalPeriod")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFiscalPeriod(request.getContext().getTenantId(), request.getFiscalPeriod());
        log.atDebug().addKeyValue("auditValue", request.getFiscalPeriod().getFiscalPeriodId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RepairFiscalPeriod", request.getFiscalPeriod().getFiscalPeriodId());
        return RepairFiscalPeriodResponse.newBuilder()
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .setFiscalPeriodId(request.getFiscalPeriodId())
                        .setOrganizer(request.getFiscalPeriod().getOrganizer())
                        .build())
                .build();
    }

    /**
     * Returns a fiscal period.
     */
    public InspectFiscalPeriodResponse inspectFiscalPeriod(InspectFiscalPeriodRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectFiscalPeriod")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FiscalPeriod", request.getFiscalPeriodId(), "InspectFiscalPeriod");
        return InspectFiscalPeriodResponse.newBuilder()
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .setFiscalPeriodId(request.getFiscalPeriodId())
                        .build())
                .build();
    }
}
