package com.acme.finance.domains.banking;

import com.acme.banking.v1.AdvanceStandingOrderWorkflowRequest;
import com.acme.banking.v1.AdvanceStandingOrderWorkflowResponse;
import com.acme.banking.v1.BankAccount;
import com.acme.banking.v1.CancelStandingOrderWorkflowRequest;
import com.acme.banking.v1.CancelStandingOrderWorkflowResponse;
import com.acme.banking.v1.DelegateStandingOrderApprovalRequest;
import com.acme.banking.v1.DelegateStandingOrderApprovalResponse;
import com.acme.banking.v1.EscalateMandateRequest;
import com.acme.banking.v1.EscalateMandateResponse;
import com.acme.banking.v1.InspectStandingOrderWorkflowRequest;
import com.acme.banking.v1.InspectStandingOrderWorkflowResponse;
import com.acme.banking.v1.ListStandingOrderWorkflowsRequest;
import com.acme.banking.v1.ListStandingOrderWorkflowsResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateApprovalServiceGrpc;
import com.acme.banking.v1.MandateValidationServiceGrpc;
import com.acme.banking.v1.PauseStandingOrderWorkflowRequest;
import com.acme.banking.v1.PauseStandingOrderWorkflowResponse;
import com.acme.banking.v1.ResumeStandingOrderWorkflowRequest;
import com.acme.banking.v1.ResumeStandingOrderWorkflowResponse;
import com.acme.banking.v1.RetryStandingOrderWorkflowRequest;
import com.acme.banking.v1.RetryStandingOrderWorkflowResponse;
import com.acme.banking.v1.RetryStatementWorkflowRequest;
import com.acme.banking.v1.RetryStatementWorkflowResponse;
import com.acme.banking.v1.ScoreMandateRequest;
import com.acme.banking.v1.ScoreMandateResponse;
import com.acme.banking.v1.SignalStandingOrderWorkflowRequest;
import com.acme.banking.v1.SignalStandingOrderWorkflowResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderApprovalServiceGrpc;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StartStandingOrderWorkflowRequest;
import com.acme.banking.v1.StartStandingOrderWorkflowResponse;
import com.acme.banking.v1.Statement;
import com.acme.banking.v1.StatementWorkflowServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.LintCustomersRequest;
import com.acme.customers.v1.LintCustomersResponse;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.documents.v1.ValidateDocumentRequest;
import com.acme.documents.v1.ValidateDocumentResponse;
import com.acme.employees.v1.CloneEmployeeRequest;
import com.acme.employees.v1.CloneEmployeeResponse;
import com.acme.employees.v1.Employee;
import com.acme.employees.v1.EmployeeServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ApprovePremiumRequest;
import com.acme.insurance.v1.ApprovePremiumResponse;
import com.acme.insurance.v1.CancelPolicyWorkflowRequest;
import com.acme.insurance.v1.CancelPolicyWorkflowResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimLifecycleServiceGrpc;
import com.acme.insurance.v1.ClaimWorkflowServiceGrpc;
import com.acme.insurance.v1.DelegatePremiumApprovalRequest;
import com.acme.insurance.v1.DelegatePremiumApprovalResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyWorkflowServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumApprovalServiceGrpc;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.ResumeClaimWorkflowRequest;
import com.acme.insurance.v1.ResumeClaimWorkflowResponse;
import com.acme.insurance.v1.ScheduleClaimExpiryRequest;
import com.acme.insurance.v1.ScheduleClaimExpiryResponse;
import com.acme.insurance.v1.SyncPremiumsRequest;
import com.acme.insurance.v1.SyncPremiumsResponse;
import com.acme.search.v1.ScoreSearchIndexRequest;
import com.acme.search.v1.ScoreSearchIndexResponse;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexValidationServiceGrpc;
import com.acme.seating.v1.CreateSeatMapRequest;
import com.acme.seating.v1.CreateSeatMapResponse;
import com.acme.seating.v1.SeatMap;
import com.acme.seating.v1.SeatMapServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.venues.v1.ValidateVenueRequest;
import com.acme.venues.v1.ValidateVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class StandingOrderWorkflowApplicationTest {
    @Test
    void retryStandingOrderWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BankingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankAccount(String tenant, BankAccount value) {}
            public void saveMandate(String tenant, Mandate value) {}
            public void saveStandingOrder(String tenant, StandingOrder value) {}
            public void saveStatement(String tenant, Statement value) {}
        };
        var application = new StandingOrderWorkflowApplication(
                MandateApprovalServiceGrpc.newBlockingStub(channel),
                MandateValidationServiceGrpc.newBlockingStub(channel),
                StandingOrderApprovalServiceGrpc.newBlockingStub(channel),
                StatementWorkflowServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CustomerValidationServiceGrpc.newBlockingStub(channel),
                DocumentValidationServiceGrpc.newBlockingStub(channel),
                EmployeeServiceGrpc.newBlockingStub(channel),
                ClaimLifecycleServiceGrpc.newBlockingStub(channel),
                ClaimWorkflowServiceGrpc.newBlockingStub(channel),
                PolicyWorkflowServiceGrpc.newBlockingStub(channel),
                PremiumApprovalServiceGrpc.newBlockingStub(channel),
                PremiumSyncServiceGrpc.newBlockingStub(channel),
                repository,
                SearchIndexValidationServiceGrpc.newBlockingStub(channel),
                SeatMapServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(RetryStandingOrderWorkflowRequest.getDefaultInstance());
        var response = application.retryStandingOrderWorkflow(request);
        var sent0 = channel.request("acme.insurance.v1.ClaimLifecycleService/ScheduleClaimExpiry");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "standing_order", "organizer", "user_id"),
                ProbeSupport.value(sent0, "claim", "assignee", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "standing_order", "audit", "updated_at"),
                ProbeSupport.value(sent0, "claim", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "email"),
                ProbeSupport.value(sent0, "claim", "requester", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "standing_order", "audit", "updated_by"),
                ProbeSupport.value(sent0, "claim", "audit", "updated_by"));
        var sent1 = channel.request("acme.banking.v1.StandingOrderApprovalService/DelegateStandingOrderApproval");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "standing_order", "participants", "contact", "name"),
                ProbeSupport.value(sent1, "standing_order", "participants", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "standing_order", "participants", "contact", "email"),
                ProbeSupport.value(sent1, "standing_order", "participants", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "standing_order", "participants", "contact", "phone"),
                ProbeSupport.value(sent1, "standing_order", "participants", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "standing_order", "participants", "participant_id"),
                ProbeSupport.value(sent1, "standing_order", "participants", "participant_id"));
        var sent2 = channel.request("acme.banking.v1.StatementWorkflowService/RetryStatementWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "standing_order", "location", "lines"),
                ProbeSupport.value(sent2, "statement", "billing_address", "lines"));
        var sent3 = channel.request("acme.venues.v1.VenueValidationService/ValidateVenue");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "standing_order", "location", "postal_code"),
                ProbeSupport.value(sent3, "venue", "address", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "phone"),
                ProbeSupport.value(sent3, "details", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimLifecycleService/ScheduleClaimExpiry"), "outcome", "next_steps"),
                ProbeSupport.value(response, "outcome", "next_steps"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.StatementWorkflowService/RetryStatementWorkflow"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.venues.v1.VenueValidationService/ValidateVenue"), "venue", "audit", "created_at"),
                ProbeSupport.value(response, "standing_order", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "standing_order_id"),
                ProbeSupport.value(response, "standing_order", "standing_order_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "standing_order", "window"),
                ProbeSupport.value(response, "standing_order", "window"));
    }
}
