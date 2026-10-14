package com.acme.finance.domains.banking;

import com.acme.accounting.v1.CompareReconciliationOptionsRequest;
import com.acme.accounting.v1.CompareReconciliationOptionsResponse;
import com.acme.accounting.v1.Reconciliation;
import com.acme.accounting.v1.ReconciliationCalculationServiceGrpc;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyBlockAction;
import com.acme.authorization.v1.AccessPolicyValidationServiceGrpc;
import com.acme.authorization.v1.PreviewAccessPolicyRequest;
import com.acme.authorization.v1.PreviewAccessPolicyResponse;
import com.acme.banking.v1.ApproveStandingOrderRequest;
import com.acme.banking.v1.ApproveStandingOrderResponse;
import com.acme.banking.v1.BankAccount;
import com.acme.banking.v1.BankAccountQueryServiceGrpc;
import com.acme.banking.v1.BankAccountServiceGrpc;
import com.acme.banking.v1.CommentStatementApprovalRequest;
import com.acme.banking.v1.CommentStatementApprovalResponse;
import com.acme.banking.v1.DelegateStandingOrderApprovalRequest;
import com.acme.banking.v1.DelegateStandingOrderApprovalResponse;
import com.acme.banking.v1.EscalateStandingOrderRequest;
import com.acme.banking.v1.EscalateStandingOrderResponse;
import com.acme.banking.v1.ListBankAccountsRequest;
import com.acme.banking.v1.ListBankAccountsResponse;
import com.acme.banking.v1.ListPendingStandingOrdersRequest;
import com.acme.banking.v1.ListPendingStandingOrdersResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.banking.v1.PreviewStandingOrderRequest;
import com.acme.banking.v1.PreviewStandingOrderResponse;
import com.acme.banking.v1.RecalculateMandatesRequest;
import com.acme.banking.v1.RecalculateMandatesResponse;
import com.acme.banking.v1.RecallStandingOrderRequest;
import com.acme.banking.v1.RecallStandingOrderResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StandingOrderValidationServiceGrpc;
import com.acme.banking.v1.Statement;
import com.acme.banking.v1.StatementApprovalServiceGrpc;
import com.acme.banking.v1.SubmitStandingOrderRequest;
import com.acme.banking.v1.SubmitStandingOrderResponse;
import com.acme.banking.v1.SummarizeBankAccountsRequest;
import com.acme.banking.v1.SummarizeBankAccountsResponse;
import com.acme.banking.v1.UpdateBankAccountRequest;
import com.acme.banking.v1.UpdateBankAccountResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.ExportCustomersRequest;
import com.acme.customers.v1.ExportCustomersResponse;
import com.acme.customers.v1.ExportHouseholdsRequest;
import com.acme.customers.v1.ExportHouseholdsResponse;
import com.acme.customers.v1.HouseholdQueryServiceGrpc;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.customers.v1.SuggestHouseholdsRequest;
import com.acme.customers.v1.SuggestHouseholdsResponse;
import com.acme.customers.v1.ValidatePreferenceRequest;
import com.acme.customers.v1.ValidatePreferenceResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ApprovePremiumRequest;
import com.acme.insurance.v1.ApprovePremiumResponse;
import com.acme.insurance.v1.ImportUnderwritingsRequest;
import com.acme.insurance.v1.ImportUnderwritingsResponse;
import com.acme.insurance.v1.PremiumApprovalServiceGrpc;
import com.acme.insurance.v1.UnderwritingImportServiceGrpc;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class StandingOrderApprovalApplicationTest {
    @Test
    void recallStandingOrderPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BankingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankAccount(String tenant, BankAccount value) {}
            public void saveMandate(String tenant, Mandate value) {}
            public void saveStandingOrder(String tenant, StandingOrder value) {}
            public void saveStatement(String tenant, Statement value) {}
        };
        var listener = new StandingOrderApprovalApplicationEvents(BankAccountServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof StandingOrderApprovalApplication.EscalateStandingOrderStage2Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new StandingOrderApprovalApplication(
                ReconciliationCalculationServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                AccessPolicyValidationServiceGrpc.newBlockingStub(channel),
                BankAccountQueryServiceGrpc.newBlockingStub(channel),
                BankAccountServiceGrpc.newBlockingStub(channel),
                MandateCalculationServiceGrpc.newBlockingStub(channel),
                StandingOrderValidationServiceGrpc.newBlockingStub(channel),
                StatementApprovalServiceGrpc.newFutureStub(channel),
                CustomerQueryServiceGrpc.newBlockingStub(channel),
                HouseholdQueryServiceGrpc.newBlockingStub(channel),
                PreferenceValidationServiceGrpc.newBlockingStub(channel),
                events,
                PremiumApprovalServiceGrpc.newBlockingStub(channel),
                UnderwritingImportServiceGrpc.newStub(channel),
                repository);
        var request = ProbeSupport.sample(RecallStandingOrderRequest.getDefaultInstance());
        var response = application.recallStandingOrder(request);
        var sent0 = channel.request("acme.banking.v1.BankAccountService/UpdateBankAccount");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "standing_order", "participants", "contact", "email"),
                ProbeSupport.value(sent0, "bank_account", "holder", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "standing_order", "participants", "contact", "name"),
                ProbeSupport.value(sent0, "bank_account", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "standing_order", "participants", "contact", "preferred_channel"),
                ProbeSupport.value(sent0, "bank_account", "contact", "preferred_channel"));
        var sent1 = channel.request("acme.customers.v1.HouseholdQueryService/ExportHouseholds");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "standing_order", "location", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.BankAccountService/UpdateBankAccount"), "bank_account", "labels"),
                ProbeSupport.value(sent1, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.BankAccountService/UpdateBankAccount"), "bank_account", "audit", "created_at"),
                ProbeSupport.value(sent1, "filter", "created_after"));
        var sent2 = channel.request("acme.banking.v1.StatementApprovalService/CommentStatementApproval");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "standing_order", "location", "postal_code"),
                ProbeSupport.value(sent2, "statement", "billing_address", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.BankAccountService/UpdateBankAccount"), "bank_account", "audit", "etag"),
                ProbeSupport.value(sent2, "statement", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.StatementApprovalService/CommentStatementApproval"), "outcome", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "standing_order_id"),
                ProbeSupport.value(response, "standing_order", "standing_order_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "standing_order", "window"),
                ProbeSupport.value(response, "standing_order", "window"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new StandingOrderApprovalApplicationEvents(BankAccountServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<ListBankAccountsResponse>();
        listener.on(new StandingOrderApprovalApplication.EscalateStandingOrderStage2Requested(ProbeSupport.sample(ListBankAccountsRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
