package com.acme.finance.domains.compliance;

import com.acme.banking.v1.BankAccountQueryServiceGrpc;
import com.acme.banking.v1.BrowseBankAccountsRequest;
import com.acme.banking.v1.BrowseBankAccountsResponse;
import com.acme.banking.v1.FindBankAccountsRequest;
import com.acme.banking.v1.FindBankAccountsResponse;
import com.acme.chat.v1.ChatRoom;
import com.acme.chat.v1.ChatRoomCalculationServiceGrpc;
import com.acme.chat.v1.CompareChatRoomOptionsRequest;
import com.acme.chat.v1.CompareChatRoomOptionsResponse;
import com.acme.chat.v1.RecalculateChatRoomsRequest;
import com.acme.chat.v1.RecalculateChatRoomsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.CheckComplianceCaseEligibilityRequest;
import com.acme.compliance.v1.CheckComplianceCaseEligibilityResponse;
import com.acme.compliance.v1.CompareComplianceCaseOptionsRequest;
import com.acme.compliance.v1.CompareComplianceCaseOptionsResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseSla;
import com.acme.compliance.v1.ComplianceCaseStatusChange;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlBlockAction;
import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.compliance.v1.ExplainComplianceCaseCalculationRequest;
import com.acme.compliance.v1.ExplainComplianceCaseCalculationResponse;
import com.acme.compliance.v1.ExplainComplianceCaseDecisionRequest;
import com.acme.compliance.v1.ExplainComplianceCaseDecisionResponse;
import com.acme.compliance.v1.GetComplianceCaseRulesRequest;
import com.acme.compliance.v1.GetComplianceCaseRulesResponse;
import com.acme.compliance.v1.LintComplianceCasesRequest;
import com.acme.compliance.v1.LintComplianceCasesResponse;
import com.acme.compliance.v1.NormalizeComplianceCaseRequest;
import com.acme.compliance.v1.NormalizeComplianceCaseResponse;
import com.acme.compliance.v1.PreviewComplianceCaseRequest;
import com.acme.compliance.v1.PreviewComplianceCaseResponse;
import com.acme.compliance.v1.QuoteControlRequest;
import com.acme.compliance.v1.QuoteControlResponse;
import com.acme.compliance.v1.RecalculateControlsRequest;
import com.acme.compliance.v1.RecalculateControlsResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SimulateComplianceCaseRequest;
import com.acme.compliance.v1.SimulateComplianceCaseResponse;
import com.acme.compliance.v1.ValidateComplianceCaseRequest;
import com.acme.compliance.v1.ValidateComplianceCaseResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.BrowsePoliciesRequest;
import com.acme.insurance.v1.BrowsePoliciesResponse;
import com.acme.insurance.v1.PolicyQueryServiceGrpc;
import com.acme.ledger.v1.JournalEntryNotificationServiceGrpc;
import com.acme.ledger.v1.SubscribeJournalEntryUpdatesRequest;
import com.acme.ledger.v1.SubscribeJournalEntryUpdatesResponse;
import com.acme.lending.v1.CalculateCollateralRequest;
import com.acme.lending.v1.CalculateCollateralResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralCalculationServiceGrpc;
import com.acme.lending.v1.CollateralDimensions;
import com.acme.lending.v1.SimulateCollateralRequest;
import com.acme.lending.v1.SimulateCollateralResponse;
import com.acme.messaging.v1.ChannelRef;
import com.acme.messaging.v1.CompareConversationOptionsRequest;
import com.acme.messaging.v1.CompareConversationOptionsResponse;
import com.acme.messaging.v1.Conversation;
import com.acme.messaging.v1.ConversationCalculationServiceGrpc;
import com.google.protobuf.Any;
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

class ComplianceCaseValidationApplicationTest {
    @Test
    void validateComplianceCasePreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ComplianceRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAttestation(String tenant, Attestation value) {}
            public void saveComplianceCase(String tenant, ComplianceCase value) {}
            public void saveControl(String tenant, Control value) {}
            public void saveSanction(String tenant, Sanction value) {}
        };
        var application = new ComplianceCaseValidationApplication(
                BankAccountQueryServiceGrpc.newBlockingStub(channel),
                BankAccountQueryServiceGrpc.newFutureStub(channel),
                ChatRoomCalculationServiceGrpc.newBlockingStub(channel),
                ComplianceCaseCalculationServiceGrpc.newBlockingStub(channel),
                ControlCalculationServiceGrpc.newBlockingStub(channel),
                PolicyQueryServiceGrpc.newBlockingStub(channel),
                JournalEntryNotificationServiceGrpc.newBlockingStub(channel),
                CollateralCalculationServiceGrpc.newBlockingStub(channel),
                ConversationCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(ValidateComplianceCaseRequest.getDefaultInstance());
        var response = application.validateComplianceCase(request);
        var sent0 = channel.request("acme.chat.v1.ChatRoomCalculationService/RecalculateChatRooms");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "compliance_case", "audit", "etag"),
                ProbeSupport.value(sent0, "chat_room", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "compliance_case", "audit", "created_at"),
                ProbeSupport.value(sent0, "chat_room", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "compliance_case", "audit", "updated_at"),
                ProbeSupport.value(sent0, "chat_room", "audit", "updated_at"));
        var sent1 = channel.request("acme.lending.v1.CollateralCalculationService/CalculateCollateral");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "compliance_case", "audit", "updated_by"),
                ProbeSupport.value(sent1, "collateral", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.chat.v1.ChatRoomCalculationService/RecalculateChatRooms"), "chat_room", "address", "country_code"),
                ProbeSupport.value(sent1, "collateral", "address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.chat.v1.ChatRoomCalculationService/RecalculateChatRooms"), "chat_room", "address", "lines"),
                ProbeSupport.value(sent1, "details", "destination", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.chat.v1.ChatRoomCalculationService/RecalculateChatRooms"), "chat_room", "dimensions", "height"),
                ProbeSupport.value(sent1, "collateral", "dimensions", "height"));
        var sent2 = channel.request("acme.messaging.v1.ConversationCalculationService/CompareConversationOptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralCalculationService/CalculateCollateral"), "collateral", "owner", "display_name"),
                ProbeSupport.value(sent2, "conversation", "assignee", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralCalculationService/CalculateCollateral"), "collateral", "address", "recipient"),
                ProbeSupport.value(sent2, "details", "destination", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "compliance_case", "customer", "display_name"),
                ProbeSupport.value(sent2, "conversation", "customer", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralCalculationService/CalculateCollateral"), "collateral", "address", "locality"),
                ProbeSupport.value(sent2, "details", "destination", "locality"));
        var sent3 = channel.request("acme.compliance.v1.ComplianceCaseCalculationService/CompareComplianceCaseOptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "compliance_case", "sla", "paused_for"),
                ProbeSupport.value(sent3, "compliance_case", "sla", "paused_for"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralCalculationService/CalculateCollateral"), "collateral", "owner", "email"),
                ProbeSupport.value(sent3, "compliance_case", "assignee", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.chat.v1.ChatRoomCalculationService/RecalculateChatRooms"), "chat_room", "address", "lines"),
                ProbeSupport.value(sent3, "details", "destination", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.chat.v1.ChatRoomCalculationService/RecalculateChatRooms"), "chat_room", "audit", "etag"),
                ProbeSupport.value(response, "compliance_case", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralCalculationService/CalculateCollateral"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.messaging.v1.ConversationCalculationService/CompareConversationOptions"), "conversation", "assignee", "user_id"),
                ProbeSupport.value(response, "compliance_case", "assignee", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ComplianceCaseCalculationService/CompareComplianceCaseOptions"), "compliance_case", "sla", "breached"),
                ProbeSupport.value(response, "compliance_case", "sla", "breached"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "compliance_case_id"),
                ProbeSupport.value(response, "compliance_case", "compliance_case_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "compliance_case", "priority"),
                ProbeSupport.value(response, "compliance_case", "priority"));
    }
}
