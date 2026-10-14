package com.acme.finance.domains.ledger;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.ArchiveFiscalPeriodRequest;
import com.acme.ledger.v1.ArchiveFiscalPeriodResponse;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.InspectFiscalPeriodRequest;
import com.acme.ledger.v1.InspectFiscalPeriodResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.ledger.v1.LockFiscalPeriodRequest;
import com.acme.ledger.v1.LockFiscalPeriodResponse;
import com.acme.ledger.v1.PostingBatch;
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
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class FiscalPeriodAdminApplicationTest {
    @Test
    void reassignFiscalPeriodPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new LedgerRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveFiscalPeriod(String tenant, FiscalPeriod value) {}
            public void saveJournalEntry(String tenant, JournalEntry value) {}
            public void saveLedgerAccount(String tenant, LedgerAccount value) {}
            public void savePostingBatch(String tenant, PostingBatch value) {}
        };
        var application = new FiscalPeriodAdminApplication(
                ChannelRegistry.fixed(channel),
                LedgerAccountHistoryServiceGrpc.newBlockingStub(channel),
                LedgerAccountHistoryServiceGrpc.newFutureStub(channel),
                repository);
        var request = ProbeSupport.sample(ReassignFiscalPeriodRequest.getDefaultInstance());
        var response = application.reassignFiscalPeriod(request);
        var sent0 = channel.request("acme.ledger.v1.LedgerAccountHistoryService/RecordLedgerAccountVersion");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "assignee", "email"),
                ProbeSupport.value(sent0, "ledger_account", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "assignee", "display_name"),
                ProbeSupport.value(sent0, "ledger_account", "holder", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "annotations"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountHistoryService/RecordLedgerAccountVersion"), "outcome", "next_steps"),
                ProbeSupport.value(response, "outcome", "next_steps"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fiscal_period_id"),
                ProbeSupport.value(response, "fiscal_period", "fiscal_period_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "assignee", "display_name"),
                ProbeSupport.value(response, "fiscal_period", "organizer", "display_name"));
    }
}
