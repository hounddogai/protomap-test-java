package com.acme.finance.domains.billing;

import com.acme.accounting.v1.BatchCreateChartOfAccountsesRequest;
import com.acme.accounting.v1.BatchCreateChartOfAccountsesResponse;
import com.acme.accounting.v1.ChartOfAccountsServiceGrpc;
import com.acme.audit.v1.ListRetentionPolicyVersionsRequest;
import com.acme.audit.v1.ListRetentionPolicyVersionsResponse;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.SendRetentionPolicyTestNoticeRequest;
import com.acme.audit.v1.SendRetentionPolicyTestNoticeResponse;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.CountAccessPoliciesRequest;
import com.acme.authorization.v1.CountAccessPoliciesResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.SubmitMandateRequest;
import com.acme.banking.v1.SubmitMandateResponse;
import com.acme.billing.v1.ActivateCreditNoteRequest;
import com.acme.billing.v1.ActivateCreditNoteResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.CalculateInvoiceRequest;
import com.acme.billing.v1.CalculateInvoiceResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.CreditNoteCalculationServiceGrpc;
import com.acme.billing.v1.CreditNoteLine;
import com.acme.billing.v1.Dunning;
import com.acme.billing.v1.ExpireCreditNoteRequest;
import com.acme.billing.v1.ExpireCreditNoteResponse;
import com.acme.billing.v1.ExplainPaymentTermCalculationRequest;
import com.acme.billing.v1.ExplainPaymentTermCalculationResponse;
import com.acme.billing.v1.GetCreditNoteLifecycleRequest;
import com.acme.billing.v1.GetCreditNoteLifecycleResponse;
import com.acme.billing.v1.GetCreditNoteRatesRequest;
import com.acme.billing.v1.GetCreditNoteRatesResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceBankSettlement;
import com.acme.billing.v1.InvoiceCalculationServiceGrpc;
import com.acme.billing.v1.InvoiceRef;
import com.acme.billing.v1.InvoiceValidationServiceGrpc;
import com.acme.billing.v1.NormalizeInvoiceRequest;
import com.acme.billing.v1.NormalizeInvoiceResponse;
import com.acme.billing.v1.PaymentTerm;
import com.acme.billing.v1.PaymentTermCalculationServiceGrpc;
import com.acme.billing.v1.ReinstateCreditNoteRequest;
import com.acme.billing.v1.ReinstateCreditNoteResponse;
import com.acme.billing.v1.RenewCreditNoteRequest;
import com.acme.billing.v1.RenewCreditNoteResponse;
import com.acme.billing.v1.ScheduleCreditNoteExpiryRequest;
import com.acme.billing.v1.ScheduleCreditNoteExpiryResponse;
import com.acme.billing.v1.ScoreInvoiceRequest;
import com.acme.billing.v1.ScoreInvoiceResponse;
import com.acme.billing.v1.SimulateInvoiceRequest;
import com.acme.billing.v1.SimulateInvoiceResponse;
import com.acme.billing.v1.SuspendCreditNoteRequest;
import com.acme.billing.v1.SuspendCreditNoteResponse;
import com.acme.billing.v1.TerminateCreditNoteRequest;
import com.acme.billing.v1.TerminateCreditNoteResponse;
import com.acme.budgeting.v1.StreamVarianceChangesRequest;
import com.acme.budgeting.v1.StreamVarianceChangesResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceComment;
import com.acme.budgeting.v1.VarianceSyncServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.PreviewHouseholdRequest;
import com.acme.customers.v1.PreviewHouseholdResponse;
import com.acme.customers.v1.QuoteCustomerRequest;
import com.acme.customers.v1.QuoteCustomerResponse;
import com.acme.customers.v1.QuotePreferenceRequest;
import com.acme.customers.v1.QuotePreferenceResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UpdateUserRequest;
import com.acme.identity.v1.UpdateUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountBankFunding;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.ledger.v1.LedgerAccountValidationServiceGrpc;
import com.acme.ledger.v1.PostingBatchRef;
import com.acme.ledger.v1.RevertLedgerAccountRequest;
import com.acme.ledger.v1.RevertLedgerAccountResponse;
import com.acme.ledger.v1.ScoreLedgerAccountRequest;
import com.acme.ledger.v1.ScoreLedgerAccountResponse;
import com.acme.notifications.v1.CreateNotificationRequest;
import com.acme.notifications.v1.CreateNotificationResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.notifications.v1.NotificationTemplateRef;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class CreditNoteLifecycleApplicationTest {
    @Test
    void expireCreditNotePreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BillingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBillingAccount(String tenant, BillingAccount value) {}
            public void saveCreditNote(String tenant, CreditNote value) {}
            public void saveDunning(String tenant, Dunning value) {}
            public void saveInvoice(String tenant, Invoice value) {}
            public void savePaymentTerm(String tenant, PaymentTerm value) {}
        };
        var application = new CreditNoteLifecycleApplication(
                ChartOfAccountsServiceGrpc.newStub(channel),
                RetentionPolicyHistoryServiceGrpc.newBlockingStub(channel),
                RetentionPolicyNotificationServiceGrpc.newBlockingStub(channel),
                AccessPolicyQueryServiceGrpc.newBlockingStub(channel),
                CreditNoteCalculationServiceGrpc.newBlockingStub(channel),
                InvoiceCalculationServiceGrpc.newBlockingStub(channel),
                InvoiceCalculationServiceGrpc.newFutureStub(channel),
                InvoiceValidationServiceGrpc.newBlockingStub(channel),
                InvoiceValidationServiceGrpc.newFutureStub(channel),
                PaymentTermCalculationServiceGrpc.newBlockingStub(channel),
                VarianceSyncServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                CustomerCalculationServiceGrpc.newBlockingStub(channel),
                HouseholdValidationServiceGrpc.newBlockingStub(channel),
                PreferenceCalculationServiceGrpc.newBlockingStub(channel),
                UserServiceGrpc.newBlockingStub(channel),
                LedgerAccountHistoryServiceGrpc.newBlockingStub(channel),
                LedgerAccountValidationServiceGrpc.newBlockingStub(channel),
                NotificationServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(ExpireCreditNoteRequest.getDefaultInstance());
        var response = application.expireCreditNote(request);
        var sent0 = channel.request("acme.billing.v1.CreditNoteCalculationService/GetCreditNoteRates");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "credit_note_id"),
                ProbeSupport.value(sent0, "credit_note_id"));
        var sent1 = channel.request("acme.billing.v1.InvoiceCalculationService/SimulateInvoice");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.CreditNoteCalculationService/GetCreditNoteRates"), "credit_note", "audit", "etag"),
                ProbeSupport.value(sent1, "invoice", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.CreditNoteCalculationService/GetCreditNoteRates"), "credit_note", "bank", "bic"),
                ProbeSupport.value(sent1, "invoice", "bank", "bic"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.CreditNoteCalculationService/GetCreditNoteRates"), "credit_note", "billing_address", "country_code"),
                ProbeSupport.value(sent1, "invoice", "billing_address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.CreditNoteCalculationService/GetCreditNoteRates"), "credit_note", "bank", "iban"),
                ProbeSupport.value(sent1, "invoice", "bank", "iban"));
        var sent2 = channel.request("acme.notifications.v1.NotificationService/CreateNotification");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceCalculationService/SimulateInvoice"), "invoice", "audit", "etag"),
                ProbeSupport.value(sent2, "notification", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceCalculationService/SimulateInvoice"), "invoice", "counterparty", "display_name"),
                ProbeSupport.value(sent2, "notification", "customer", "display_name"));
        var sent3 = channel.request("acme.billing.v1.PaymentTermCalculationService/ExplainPaymentTermCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationService/CreateNotification"), "notification", "audit", "updated_at"),
                ProbeSupport.value(sent3, "payment_term", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.CreditNoteCalculationService/GetCreditNoteRates"), "credit_note", "audit", "updated_by"),
                ProbeSupport.value(response, "credit_note", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.CreditNoteCalculationService/GetCreditNoteRates"), "credit_note", "billing_address", "region"),
                ProbeSupport.value(response, "credit_note", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceCalculationService/SimulateInvoice"), "invoice", "billing_address", "locality"),
                ProbeSupport.value(response, "credit_note", "billing_address", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationService/CreateNotification"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationService/CreateNotification"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "credit_note_id"),
                ProbeSupport.value(response, "credit_note", "credit_note_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
    }
}
