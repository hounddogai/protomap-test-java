package com.acme.finance.domains.banking;

import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.banking.v1.ApproveStatementRequest;
import com.acme.banking.v1.ApproveStatementResponse;
import com.acme.banking.v1.BankAccount;
import com.acme.banking.v1.CommentStatementApprovalRequest;
import com.acme.banking.v1.CommentStatementApprovalResponse;
import com.acme.banking.v1.CompareMandateOptionsRequest;
import com.acme.banking.v1.CompareMandateOptionsResponse;
import com.acme.banking.v1.EscalateStatementRequest;
import com.acme.banking.v1.EscalateStatementResponse;
import com.acme.banking.v1.ExplainStandingOrderDecisionRequest;
import com.acme.banking.v1.ExplainStandingOrderDecisionResponse;
import com.acme.banking.v1.GetStatementApprovalStatusRequest;
import com.acme.banking.v1.GetStatementApprovalStatusResponse;
import com.acme.banking.v1.LintStandingOrdersRequest;
import com.acme.banking.v1.LintStandingOrdersResponse;
import com.acme.banking.v1.ListPendingStatementsRequest;
import com.acme.banking.v1.ListPendingStatementsResponse;
import com.acme.banking.v1.LookupStatementRequest;
import com.acme.banking.v1.LookupStatementResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.banking.v1.MandateCardSettlement;
import com.acme.banking.v1.MandateValidationServiceGrpc;
import com.acme.banking.v1.NormalizeStandingOrderRequest;
import com.acme.banking.v1.NormalizeStandingOrderResponse;
import com.acme.banking.v1.PreviewStandingOrderRequest;
import com.acme.banking.v1.PreviewStandingOrderResponse;
import com.acme.banking.v1.RecallStatementRequest;
import com.acme.banking.v1.RecallStatementResponse;
import com.acme.banking.v1.RejectStatementRequest;
import com.acme.banking.v1.RejectStatementResponse;
import com.acme.banking.v1.ScoreStandingOrderRequest;
import com.acme.banking.v1.ScoreStandingOrderResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StandingOrderRef;
import com.acme.banking.v1.StandingOrderValidationServiceGrpc;
import com.acme.banking.v1.Statement;
import com.acme.banking.v1.StatementQueryServiceGrpc;
import com.acme.banking.v1.SubmitStatementRequest;
import com.acme.banking.v1.SubmitStatementResponse;
import com.acme.banking.v1.SummarizeStatementsRequest;
import com.acme.banking.v1.SummarizeStatementsResponse;
import com.acme.banking.v1.ValidateMandateRequest;
import com.acme.banking.v1.ValidateMandateResponse;
import com.acme.banking.v1.ValidateStandingOrderRequest;
import com.acme.banking.v1.ValidateStandingOrderResponse;
import com.acme.certificates.v1.CertificateServiceGrpc;
import com.acme.certificates.v1.ListCertificatesRequest;
import com.acme.certificates.v1.ListCertificatesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordCalculationServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.EstimateConsentRecordRequest;
import com.acme.customers.v1.EstimateConsentRecordResponse;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.customers.v1.PreviewPreferenceRequest;
import com.acme.customers.v1.PreviewPreferenceResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ClaimRef;
import com.acme.insurance.v1.ImportUnderwritingsRequest;
import com.acme.insurance.v1.ImportUnderwritingsResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingImportServiceGrpc;
import com.acme.ledger.v1.JournalEntryNotificationServiceGrpc;
import com.acme.ledger.v1.UnsubscribeJournalEntryUpdatesRequest;
import com.acme.ledger.v1.UnsubscribeJournalEntryUpdatesResponse;
import com.acme.procurement.v1.BatchCreatePurchaseRequisitionsRequest;
import com.acme.procurement.v1.BatchCreatePurchaseRequisitionsResponse;
import com.acme.procurement.v1.PurchaseRequisitionServiceGrpc;
import com.google.protobuf.Any;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class StatementApprovalApplicationTest {
    @Test
    void recallStatementPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BankingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankAccount(String tenant, BankAccount value) {}
            public void saveMandate(String tenant, Mandate value) {}
            public void saveStandingOrder(String tenant, StandingOrder value) {}
            public void saveStatement(String tenant, Statement value) {}
        };
        var listener = new StatementApprovalApplicationEvents(JournalEntryNotificationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof StatementApprovalApplication.SubmitStatementStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new StatementApprovalApplication(
                RetentionPolicySyncServiceGrpc.newStub(channel),
                MandateCalculationServiceGrpc.newBlockingStub(channel),
                MandateValidationServiceGrpc.newBlockingStub(channel),
                StandingOrderValidationServiceGrpc.newBlockingStub(channel),
                StatementQueryServiceGrpc.newBlockingStub(channel),
                CertificateServiceGrpc.newBlockingStub(channel),
                ConsentRecordCalculationServiceGrpc.newBlockingStub(channel),
                PreferenceValidationServiceGrpc.newBlockingStub(channel),
                events,
                UnderwritingImportServiceGrpc.newStub(channel),
                PurchaseRequisitionServiceGrpc.newStub(channel),
                repository);
        var request = ProbeSupport.sample(RecallStatementRequest.getDefaultInstance());
        var response = application.recallStatement(request);
        var sent0 = channel.request("acme.banking.v1.StandingOrderValidationService/PreviewStandingOrder");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "email"),
                ProbeSupport.value(sent0, "standing_order", "organizer", "email"));
        var sent1 = channel.request("acme.banking.v1.StandingOrderValidationService/NormalizeStandingOrder");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.StandingOrderValidationService/PreviewStandingOrder"), "standing_order", "organizer", "email"),
                ProbeSupport.value(sent1, "standing_order", "participants", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.StandingOrderValidationService/PreviewStandingOrder"), "standing_order", "location", "country_code"),
                ProbeSupport.value(sent1, "standing_order", "location", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "statement", "billing_address", "postal_code"),
                ProbeSupport.value(sent1, "standing_order", "location", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "statement", "audit", "created_by"),
                ProbeSupport.value(sent1, "standing_order", "audit", "created_by"));
        var sent2 = channel.request("acme.certificates.v1.CertificateService/ListCertificates");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "statement", "billing_address", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        var sent3 = channel.request("acme.banking.v1.StatementQueryService/SummarizeStatements");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "statement", "statement_id"),
                ProbeSupport.value(sent3, "statement_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent3, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.StandingOrderValidationService/PreviewStandingOrder"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.StandingOrderValidationService/NormalizeStandingOrder"), "standing_order", "location", "postal_code"),
                ProbeSupport.value(response, "statement", "billing_address", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.certificates.v1.CertificateService/ListCertificates"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.certificates.v1.CertificateService/ListCertificates"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "statement_id"),
                ProbeSupport.value(response, "statement", "statement_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "statement", "total"),
                ProbeSupport.value(response, "statement", "total"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new StatementApprovalApplicationEvents(JournalEntryNotificationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<UnsubscribeJournalEntryUpdatesResponse>();
        listener.on(new StatementApprovalApplication.SubmitStatementStage0Requested(ProbeSupport.sample(UnsubscribeJournalEntryUpdatesRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
