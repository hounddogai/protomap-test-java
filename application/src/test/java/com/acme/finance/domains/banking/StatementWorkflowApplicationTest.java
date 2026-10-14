package com.acme.finance.domains.banking;

import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.CreateAccessPolicyRequest;
import com.acme.authorization.v1.CreateAccessPolicyResponse;
import com.acme.banking.v1.AdvanceStatementWorkflowRequest;
import com.acme.banking.v1.AdvanceStatementWorkflowResponse;
import com.acme.banking.v1.ApproveStatementRequest;
import com.acme.banking.v1.ApproveStatementResponse;
import com.acme.banking.v1.BankAccount;
import com.acme.banking.v1.BankAccountQueryServiceGrpc;
import com.acme.banking.v1.BankAccountWorkflowServiceGrpc;
import com.acme.banking.v1.CalculateMandateRequest;
import com.acme.banking.v1.CalculateMandateResponse;
import com.acme.banking.v1.CancelStatementWorkflowRequest;
import com.acme.banking.v1.CancelStatementWorkflowResponse;
import com.acme.banking.v1.InspectStatementWorkflowRequest;
import com.acme.banking.v1.InspectStatementWorkflowResponse;
import com.acme.banking.v1.ListStatementWorkflowsRequest;
import com.acme.banking.v1.ListStatementWorkflowsResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.banking.v1.MandateLine;
import com.acme.banking.v1.MandateLineTax;
import com.acme.banking.v1.MandateValidationServiceGrpc;
import com.acme.banking.v1.NormalizeMandateRequest;
import com.acme.banking.v1.NormalizeMandateResponse;
import com.acme.banking.v1.PauseStatementWorkflowRequest;
import com.acme.banking.v1.PauseStatementWorkflowResponse;
import com.acme.banking.v1.RecallStandingOrderRequest;
import com.acme.banking.v1.RecallStandingOrderResponse;
import com.acme.banking.v1.RejectStatementRequest;
import com.acme.banking.v1.RejectStatementResponse;
import com.acme.banking.v1.ResumeBankAccountWorkflowRequest;
import com.acme.banking.v1.ResumeBankAccountWorkflowResponse;
import com.acme.banking.v1.ResumeStatementWorkflowRequest;
import com.acme.banking.v1.ResumeStatementWorkflowResponse;
import com.acme.banking.v1.RetryStatementWorkflowRequest;
import com.acme.banking.v1.RetryStatementWorkflowResponse;
import com.acme.banking.v1.SignalStatementWorkflowRequest;
import com.acme.banking.v1.SignalStatementWorkflowResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderApprovalServiceGrpc;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StartStatementWorkflowRequest;
import com.acme.banking.v1.StartStatementWorkflowResponse;
import com.acme.banking.v1.Statement;
import com.acme.banking.v1.StatementApprovalServiceGrpc;
import com.acme.banking.v1.StatementAttachment;
import com.acme.banking.v1.StatementCardSettlement;
import com.acme.banking.v1.StatementLine;
import com.acme.banking.v1.SummarizeBankAccountsRequest;
import com.acme.banking.v1.SummarizeBankAccountsResponse;
import com.acme.catalog.v1.BatchGetProductsRequest;
import com.acme.catalog.v1.BatchGetProductsResponse;
import com.acme.catalog.v1.ProductServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CalculateCustomerSegmentRequest;
import com.acme.customers.v1.CalculateCustomerSegmentResponse;
import com.acme.customers.v1.CompareHouseholdOptionsRequest;
import com.acme.customers.v1.CompareHouseholdOptionsResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordCalculationServiceGrpc;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.ExplainCustomerDecisionRequest;
import com.acme.customers.v1.ExplainCustomerDecisionResponse;
import com.acme.customers.v1.ExportCustomersRequest;
import com.acme.customers.v1.ExportCustomersResponse;
import com.acme.customers.v1.FindCustomersRequest;
import com.acme.customers.v1.FindCustomersResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.QuoteCustomerRequest;
import com.acme.customers.v1.QuoteCustomerResponse;
import com.acme.customers.v1.QuotePreferenceRequest;
import com.acme.customers.v1.QuotePreferenceResponse;
import com.acme.customers.v1.RecalculateConsentRecordsRequest;
import com.acme.customers.v1.RecalculateConsentRecordsResponse;
import com.acme.featureflags.v1.FeatureFlagQueryServiceGrpc;
import com.acme.featureflags.v1.SuggestFeatureFlagsRequest;
import com.acme.featureflags.v1.SuggestFeatureFlagsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ClaimApprovalServiceGrpc;
import com.acme.insurance.v1.ListPendingClaimsRequest;
import com.acme.insurance.v1.ListPendingClaimsResponse;
import com.acme.insurance.v1.StreamUnderwritingMetricsRequest;
import com.acme.insurance.v1.StreamUnderwritingMetricsResponse;
import com.acme.insurance.v1.UnderwritingReportingServiceGrpc;
import com.acme.ledger.v1.JournalEntryNotificationServiceGrpc;
import com.acme.ledger.v1.ListJournalEntrySubscriptionsRequest;
import com.acme.ledger.v1.ListJournalEntrySubscriptionsResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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

class StatementWorkflowApplicationTest {
    @Test
    void pauseStatementWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BankingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankAccount(String tenant, BankAccount value) {}
            public void saveMandate(String tenant, Mandate value) {}
            public void saveStandingOrder(String tenant, StandingOrder value) {}
            public void saveStatement(String tenant, Statement value) {}
        };
        var application = new StatementWorkflowApplication(
                AccessPolicyServiceGrpc.newBlockingStub(channel),
                BankAccountQueryServiceGrpc.newBlockingStub(channel),
                BankAccountWorkflowServiceGrpc.newFutureStub(channel),
                MandateCalculationServiceGrpc.newBlockingStub(channel),
                MandateValidationServiceGrpc.newBlockingStub(channel),
                StandingOrderApprovalServiceGrpc.newBlockingStub(channel),
                StatementApprovalServiceGrpc.newBlockingStub(channel),
                ProductServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ConsentRecordCalculationServiceGrpc.newBlockingStub(channel),
                CustomerCalculationServiceGrpc.newBlockingStub(channel),
                CustomerQueryServiceGrpc.newBlockingStub(channel),
                CustomerSegmentCalculationServiceGrpc.newBlockingStub(channel),
                CustomerValidationServiceGrpc.newBlockingStub(channel),
                HouseholdCalculationServiceGrpc.newBlockingStub(channel),
                PreferenceCalculationServiceGrpc.newBlockingStub(channel),
                FeatureFlagQueryServiceGrpc.newBlockingStub(channel),
                ClaimApprovalServiceGrpc.newBlockingStub(channel),
                UnderwritingReportingServiceGrpc.newBlockingStub(channel),
                JournalEntryNotificationServiceGrpc.newFutureStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(PauseStatementWorkflowRequest.getDefaultInstance());
        var response = application.pauseStatementWorkflow(request);
        var sent0 = channel.request("acme.banking.v1.MandateCalculationService/CalculateMandate");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "statement", "lines", "tax", "exemptions"),
                ProbeSupport.value(sent0, "mandate", "lines", "tax", "exemptions"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "statement", "lines", "tax", "rate"),
                ProbeSupport.value(sent0, "mandate", "lines", "tax", "rate"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "statement", "lines", "quantity"),
                ProbeSupport.value(sent0, "mandate", "lines", "quantity"));
        var sent1 = channel.request("acme.banking.v1.StandingOrderApprovalService/RecallStandingOrder");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "phone"),
                ProbeSupport.value(sent1, "standing_order", "participants", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.MandateCalculationService/CalculateMandate"), "mandate", "counterparty", "display_name"),
                ProbeSupport.value(sent1, "standing_order", "organizer", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.MandateCalculationService/CalculateMandate"), "mandate", "billing_address", "lines"),
                ProbeSupport.value(sent1, "standing_order", "location", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.MandateCalculationService/CalculateMandate"), "mandate", "counterparty", "email"),
                ProbeSupport.value(sent1, "standing_order", "organizer", "email"));
        var sent2 = channel.request("acme.customers.v1.CustomerValidationService/ExplainCustomerDecision");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "statement", "billing_address", "postal_code"),
                ProbeSupport.value(sent2, "customer", "primary_address", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "statement", "audit", "updated_at"),
                ProbeSupport.value(sent2, "customer", "audit", "updated_at"));
        var sent3 = channel.request("acme.banking.v1.BankAccountWorkflowService/ResumeBankAccountWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.StandingOrderApprovalService/RecallStandingOrder"), "standing_order", "audit", "created_by"),
                ProbeSupport.value(sent3, "bank_account", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.StandingOrderApprovalService/RecallStandingOrder"), "standing_order", "audit", "created_at"),
                ProbeSupport.value(response, "statement", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.StandingOrderApprovalService/RecallStandingOrder"), "standing_order", "audit", "created_by"),
                ProbeSupport.value(response, "statement", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerValidationService/ExplainCustomerDecision"), "customer", "primary_address", "locality"),
                ProbeSupport.value(response, "statement", "billing_address", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerValidationService/ExplainCustomerDecision"), "customer", "attributes"),
                ProbeSupport.value(response, "statement", "lines", "attributes"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.BankAccountWorkflowService/ResumeBankAccountWorkflow"), "bank_account", "audit", "updated_at"),
                ProbeSupport.value(response, "statement", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "statement_id"),
                ProbeSupport.value(response, "statement", "statement_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "statement", "total"),
                ProbeSupport.value(response, "statement", "total"));
    }
}
