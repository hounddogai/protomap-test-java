package com.acme.finance.domains.invoicing;

import com.acme.audit.v1.AuditEventExportServiceGrpc;
import com.acme.audit.v1.ListAuditEventExportsRequest;
import com.acme.audit.v1.ListAuditEventExportsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.ContractClause;
import com.acme.contracts.v1.ContractClauseWorkflowServiceGrpc;
import com.acme.contracts.v1.PauseContractClauseWorkflowRequest;
import com.acme.contracts.v1.PauseContractClauseWorkflowResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.NormalizeConsentRecordRequest;
import com.acme.customers.v1.NormalizeConsentRecordResponse;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.customers.v1.PreviewPreferenceRequest;
import com.acme.customers.v1.PreviewPreferenceResponse;
import com.acme.customers.v1.ResolveCustomersRequest;
import com.acme.customers.v1.ResolveCustomersResponse;
import com.acme.expenses.v1.MileageClaimRef;
import com.acme.expenses.v1.MileageClaimWorkflowServiceGrpc;
import com.acme.expenses.v1.PauseReceiptWorkflowRequest;
import com.acme.expenses.v1.PauseReceiptWorkflowResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptWorkflowServiceGrpc;
import com.acme.expenses.v1.SignalMileageClaimWorkflowRequest;
import com.acme.expenses.v1.SignalMileageClaimWorkflowResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.invoicing.v1.ActivateInvoiceTemplateRequest;
import com.acme.invoicing.v1.ActivateInvoiceTemplateResponse;
import com.acme.invoicing.v1.CalculateInvoiceBatchRequest;
import com.acme.invoicing.v1.CalculateInvoiceBatchResponse;
import com.acme.invoicing.v1.ExpireInvoiceTemplateRequest;
import com.acme.invoicing.v1.ExpireInvoiceTemplateResponse;
import com.acme.invoicing.v1.ExplainInvoiceBatchCalculationRequest;
import com.acme.invoicing.v1.ExplainInvoiceBatchCalculationResponse;
import com.acme.invoicing.v1.GetInvoiceTemplateLifecycleRequest;
import com.acme.invoicing.v1.GetInvoiceTemplateLifecycleResponse;
import com.acme.invoicing.v1.InvoiceBatch;
import com.acme.invoicing.v1.InvoiceBatchCalculationServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplate;
import com.acme.invoicing.v1.InvoiceTemplateValidationServiceGrpc;
import com.acme.invoicing.v1.NormalizeInvoiceTemplateRequest;
import com.acme.invoicing.v1.NormalizeInvoiceTemplateResponse;
import com.acme.invoicing.v1.ReinstateInvoiceTemplateRequest;
import com.acme.invoicing.v1.ReinstateInvoiceTemplateResponse;
import com.acme.invoicing.v1.RenewInvoiceTemplateRequest;
import com.acme.invoicing.v1.RenewInvoiceTemplateResponse;
import com.acme.invoicing.v1.ResolveInvoiceTemplatesRequest;
import com.acme.invoicing.v1.ResolveInvoiceTemplatesResponse;
import com.acme.invoicing.v1.ScheduleInvoiceTemplateExpiryRequest;
import com.acme.invoicing.v1.ScheduleInvoiceTemplateExpiryResponse;
import com.acme.invoicing.v1.SuspendInvoiceTemplateRequest;
import com.acme.invoicing.v1.SuspendInvoiceTemplateResponse;
import com.acme.invoicing.v1.TerminateInvoiceTemplateRequest;
import com.acme.invoicing.v1.TerminateInvoiceTemplateResponse;
import com.acme.ledger.v1.BrowseLedgerAccountsRequest;
import com.acme.ledger.v1.BrowseLedgerAccountsResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountBankFunding;
import com.acme.ledger.v1.LedgerAccountQueryServiceGrpc;
import com.acme.ledger.v1.LedgerAccountValidationServiceGrpc;
import com.acme.ledger.v1.PostingBatchRef;
import com.acme.ledger.v1.PreviewLedgerAccountRequest;
import com.acme.ledger.v1.PreviewLedgerAccountResponse;
import com.acme.ledger.v1.ValidateLedgerAccountRequest;
import com.acme.ledger.v1.ValidateLedgerAccountResponse;
import com.acme.ticketing.v1.SuggestTicketsRequest;
import com.acme.ticketing.v1.SuggestTicketsResponse;
import com.acme.ticketing.v1.TicketQueryServiceGrpc;
import com.google.protobuf.Any;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class InvoiceTemplateLifecycleRequestMapperImpl implements InvoiceTemplateLifecycleRequestMapper {
    @Override
    public CalculateInvoiceBatchRequest expireInvoiceTemplateStage1(ExpireInvoiceTemplateRequest request) {
        return CalculateInvoiceBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .build())
                .build();
    }

    @Override
    public SignalMileageClaimWorkflowRequest scheduleInvoiceTemplateExpiryStage2(ScheduleInvoiceTemplateExpiryRequest request) {
        return SignalMileageClaimWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SignalMileageClaimWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .build();
    }
}
