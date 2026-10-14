package com.acme.finance.domains.banking;

import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.StreamRetentionPolicyNoticesRequest;
import com.acme.audit.v1.StreamRetentionPolicyNoticesResponse;
import com.acme.banking.v1.AdvanceBankAccountWorkflowRequest;
import com.acme.banking.v1.AdvanceBankAccountWorkflowResponse;
import com.acme.banking.v1.ApproveMandateRequest;
import com.acme.banking.v1.ApproveMandateResponse;
import com.acme.banking.v1.BankAccount;
import com.acme.banking.v1.BankAccountBankFunding;
import com.acme.banking.v1.BankAccountLimit;
import com.acme.banking.v1.BankAccountQueryServiceGrpc;
import com.acme.banking.v1.BankAccountWorkflowServiceGrpc;
import com.acme.banking.v1.CancelStatementWorkflowRequest;
import com.acme.banking.v1.CancelStatementWorkflowResponse;
import com.acme.banking.v1.CheckStandingOrderEligibilityRequest;
import com.acme.banking.v1.CheckStandingOrderEligibilityResponse;
import com.acme.banking.v1.CommentMandateApprovalRequest;
import com.acme.banking.v1.CommentMandateApprovalResponse;
import com.acme.banking.v1.CommentStatementApprovalRequest;
import com.acme.banking.v1.CommentStatementApprovalResponse;
import com.acme.banking.v1.DelegateMandateApprovalRequest;
import com.acme.banking.v1.DelegateMandateApprovalResponse;
import com.acme.banking.v1.EscalateMandateRequest;
import com.acme.banking.v1.EscalateMandateResponse;
import com.acme.banking.v1.EscalateStandingOrderRequest;
import com.acme.banking.v1.EscalateStandingOrderResponse;
import com.acme.banking.v1.ExportStatementsRequest;
import com.acme.banking.v1.ExportStatementsResponse;
import com.acme.banking.v1.GetMandateApprovalStatusRequest;
import com.acme.banking.v1.GetMandateApprovalStatusResponse;
import com.acme.banking.v1.ListPendingMandatesRequest;
import com.acme.banking.v1.ListPendingMandatesResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.RecallMandateRequest;
import com.acme.banking.v1.RecallMandateResponse;
import com.acme.banking.v1.RejectMandateRequest;
import com.acme.banking.v1.RejectMandateResponse;
import com.acme.banking.v1.ResolveBankAccountsRequest;
import com.acme.banking.v1.ResolveBankAccountsResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderApprovalServiceGrpc;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StandingOrderValidationServiceGrpc;
import com.acme.banking.v1.Statement;
import com.acme.banking.v1.StatementApprovalServiceGrpc;
import com.acme.banking.v1.StatementCardSettlement;
import com.acme.banking.v1.StatementLine;
import com.acme.banking.v1.StatementQueryServiceGrpc;
import com.acme.banking.v1.StatementWorkflowServiceGrpc;
import com.acme.banking.v1.SubmitMandateRequest;
import com.acme.banking.v1.SubmitMandateResponse;
import com.acme.banking.v1.SuggestBankAccountsRequest;
import com.acme.banking.v1.SuggestBankAccountsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.BatchCreateComplianceCasesRequest;
import com.acme.compliance.v1.BatchCreateComplianceCasesResponse;
import com.acme.compliance.v1.ComplianceCaseServiceGrpc;
import com.acme.customers.v1.CheckPreferenceEligibilityRequest;
import com.acme.customers.v1.CheckPreferenceEligibilityResponse;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.LintCustomerSegmentsRequest;
import com.acme.customers.v1.LintCustomerSegmentsResponse;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.customers.v1.RecalculateCustomerSegmentsRequest;
import com.acme.customers.v1.RecalculateCustomerSegmentsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ApproveCoverageRequest;
import com.acme.insurance.v1.ApproveCoverageResponse;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.SubscribePostingBatchUpdatesRequest;
import com.acme.ledger.v1.SubscribePostingBatchUpdatesResponse;
import com.acme.suppliers.v1.ResolveSuppliersRequest;
import com.acme.suppliers.v1.ResolveSuppliersResponse;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class MandateApprovalApplicationTest {
    @Test
    void commentMandateApprovalPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BankingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankAccount(String tenant, BankAccount value) {}
            public void saveMandate(String tenant, Mandate value) {}
            public void saveStandingOrder(String tenant, StandingOrder value) {}
            public void saveStatement(String tenant, Statement value) {}
        };
        var application = new MandateApprovalApplication(
                RetentionPolicyNotificationServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                BankAccountQueryServiceGrpc.newBlockingStub(channel),
                BankAccountWorkflowServiceGrpc.newBlockingStub(channel),
                StandingOrderApprovalServiceGrpc.newBlockingStub(channel),
                StandingOrderValidationServiceGrpc.newBlockingStub(channel),
                StatementApprovalServiceGrpc.newBlockingStub(channel),
                StatementQueryServiceGrpc.newBlockingStub(channel),
                StatementWorkflowServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ComplianceCaseServiceGrpc.newStub(channel),
                CustomerSegmentCalculationServiceGrpc.newBlockingStub(channel),
                CustomerSegmentValidationServiceGrpc.newBlockingStub(channel),
                PreferenceValidationServiceGrpc.newBlockingStub(channel),
                PostingBatchNotificationServiceGrpc.newBlockingStub(channel),
                new MandateApprovalRequestMapperImpl(),
                repository,
                Runnable::run,
                SupplierQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(CommentMandateApprovalRequest.getDefaultInstance());
        var response = application.commentMandateApproval(request);
        var sent0 = channel.request("acme.customers.v1.CustomerSegmentCalculationService/RecalculateCustomerSegments");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "user_id"),
                ProbeSupport.value(sent0, "customer_segment", "owner", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mandate", "billing_address", "postal_code"),
                ProbeSupport.value(sent0, "details", "destination", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mandate", "billing_address", "region"),
                ProbeSupport.value(sent0, "details", "destination", "region"));
        var sent1 = channel.request("acme.banking.v1.StandingOrderApprovalService/EscalateStandingOrder");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "email"),
                ProbeSupport.value(sent1, "standing_order", "participants", "contact", "email"));
        var sent2 = channel.request("acme.insurance.v1.CoverageApprovalService/ApproveCoverage");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.StandingOrderApprovalService/EscalateStandingOrder"), "standing_order", "organizer", "email"),
                ProbeSupport.value(sent2, "details", "approver", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerSegmentCalculationService/RecalculateCustomerSegments"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.StandingOrderApprovalService/EscalateStandingOrder"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.StandingOrderApprovalService/EscalateStandingOrder"), "outcome", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.CoverageApprovalService/ApproveCoverage"), "coverage", "audit", "etag"),
                ProbeSupport.value(response, "mandate", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.CoverageApprovalService/ApproveCoverage"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mandate_id"),
                ProbeSupport.value(response, "mandate", "mandate_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
    }
}
