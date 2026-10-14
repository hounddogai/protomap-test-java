package com.acme.finance.platform.clients;

import com.acme.billing.v1.BillingAccountApprovalServiceGrpc;
import com.acme.billing.v1.BillingAccountQueryServiceGrpc;
import com.acme.billing.v1.BillingAccountServiceGrpc;
import com.acme.billing.v1.BillingAccountValidationServiceGrpc;
import com.acme.billing.v1.BillingAccountWorkflowServiceGrpc;
import com.acme.billing.v1.CreditNoteApprovalServiceGrpc;
import com.acme.billing.v1.CreditNoteCalculationServiceGrpc;
import com.acme.billing.v1.CreditNoteLifecycleServiceGrpc;
import com.acme.billing.v1.CreditNoteValidationServiceGrpc;
import com.acme.billing.v1.CreditNoteWorkflowServiceGrpc;
import com.acme.billing.v1.DunningCalculationServiceGrpc;
import com.acme.billing.v1.DunningReportingServiceGrpc;
import com.acme.billing.v1.DunningValidationServiceGrpc;
import com.acme.billing.v1.InvoiceApprovalServiceGrpc;
import com.acme.billing.v1.InvoiceCalculationServiceGrpc;
import com.acme.billing.v1.InvoiceQueryServiceGrpc;
import com.acme.billing.v1.InvoiceValidationServiceGrpc;
import com.acme.billing.v1.InvoiceWorkflowServiceGrpc;
import com.acme.billing.v1.PaymentTermApprovalServiceGrpc;
import com.acme.billing.v1.PaymentTermCalculationServiceGrpc;
import com.acme.billing.v1.PaymentTermLifecycleServiceGrpc;
import com.acme.billing.v1.PaymentTermValidationServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the billing contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class BillingClients {
    @Bean
    public BillingAccountApprovalServiceGrpc.BillingAccountApprovalServiceStub billingBillingAccountApprovalServiceAsync(ChannelRegistry channels) {
        return BillingAccountApprovalServiceGrpc.newStub(channels.lookup("acme.billing.v1.BillingAccountApprovalService"));
    }

    @Bean
    public BillingAccountApprovalServiceGrpc.BillingAccountApprovalServiceBlockingStub billingBillingAccountApprovalServiceBlocking(ChannelRegistry channels) {
        return BillingAccountApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.billing.v1.BillingAccountApprovalService"));
    }

    @Bean
    public BillingAccountQueryServiceGrpc.BillingAccountQueryServiceBlockingStub billingBillingAccountQueryServiceBlocking(ChannelRegistry channels) {
        return BillingAccountQueryServiceGrpc.newBlockingStub(channels.lookup("acme.billing.v1.BillingAccountQueryService"));
    }

    @Bean
    public BillingAccountServiceGrpc.BillingAccountServiceStub billingBillingAccountServiceAsync(ChannelRegistry channels) {
        return BillingAccountServiceGrpc.newStub(channels.lookup("acme.billing.v1.BillingAccountService"));
    }

    @Bean
    public BillingAccountServiceGrpc.BillingAccountServiceBlockingStub billingBillingAccountServiceBlocking(ChannelRegistry channels) {
        return BillingAccountServiceGrpc.newBlockingStub(channels.lookup("acme.billing.v1.BillingAccountService"));
    }

    @Bean
    public BillingAccountValidationServiceGrpc.BillingAccountValidationServiceBlockingStub billingBillingAccountValidationServiceBlocking(ChannelRegistry channels) {
        return BillingAccountValidationServiceGrpc.newBlockingStub(channels.lookup("acme.billing.v1.BillingAccountValidationService"));
    }

    @Bean
    public BillingAccountWorkflowServiceGrpc.BillingAccountWorkflowServiceBlockingStub billingBillingAccountWorkflowServiceBlocking(ChannelRegistry channels) {
        return BillingAccountWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.billing.v1.BillingAccountWorkflowService"));
    }

    @Bean
    public CreditNoteApprovalServiceGrpc.CreditNoteApprovalServiceBlockingStub billingCreditNoteApprovalServiceBlocking(ChannelRegistry channels) {
        return CreditNoteApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.billing.v1.CreditNoteApprovalService"));
    }

    @Bean
    public CreditNoteApprovalServiceGrpc.CreditNoteApprovalServiceFutureStub billingCreditNoteApprovalServiceFuture(ChannelRegistry channels) {
        return CreditNoteApprovalServiceGrpc.newFutureStub(channels.lookup("acme.billing.v1.CreditNoteApprovalService"));
    }

    @Bean
    public CreditNoteCalculationServiceGrpc.CreditNoteCalculationServiceStub billingCreditNoteCalculationServiceAsync(ChannelRegistry channels) {
        return CreditNoteCalculationServiceGrpc.newStub(channels.lookup("acme.billing.v1.CreditNoteCalculationService"));
    }

    @Bean
    public CreditNoteCalculationServiceGrpc.CreditNoteCalculationServiceBlockingStub billingCreditNoteCalculationServiceBlocking(ChannelRegistry channels) {
        return CreditNoteCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.billing.v1.CreditNoteCalculationService"));
    }

    @Bean
    public CreditNoteCalculationServiceGrpc.CreditNoteCalculationServiceFutureStub billingCreditNoteCalculationServiceFuture(ChannelRegistry channels) {
        return CreditNoteCalculationServiceGrpc.newFutureStub(channels.lookup("acme.billing.v1.CreditNoteCalculationService"));
    }

    @Bean
    public CreditNoteLifecycleServiceGrpc.CreditNoteLifecycleServiceBlockingStub billingCreditNoteLifecycleServiceBlocking(ChannelRegistry channels) {
        return CreditNoteLifecycleServiceGrpc.newBlockingStub(channels.lookup("acme.billing.v1.CreditNoteLifecycleService"));
    }

    @Bean
    public CreditNoteValidationServiceGrpc.CreditNoteValidationServiceBlockingStub billingCreditNoteValidationServiceBlocking(ChannelRegistry channels) {
        return CreditNoteValidationServiceGrpc.newBlockingStub(channels.lookup("acme.billing.v1.CreditNoteValidationService"));
    }

    @Bean
    public CreditNoteWorkflowServiceGrpc.CreditNoteWorkflowServiceBlockingStub billingCreditNoteWorkflowServiceBlocking(ChannelRegistry channels) {
        return CreditNoteWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.billing.v1.CreditNoteWorkflowService"));
    }

    @Bean
    public DunningCalculationServiceGrpc.DunningCalculationServiceBlockingStub billingDunningCalculationServiceBlocking(ChannelRegistry channels) {
        return DunningCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.billing.v1.DunningCalculationService"));
    }

    @Bean
    public DunningReportingServiceGrpc.DunningReportingServiceStub billingDunningReportingServiceAsync(ChannelRegistry channels) {
        return DunningReportingServiceGrpc.newStub(channels.lookup("acme.billing.v1.DunningReportingService"));
    }

    @Bean
    public DunningReportingServiceGrpc.DunningReportingServiceBlockingStub billingDunningReportingServiceBlocking(ChannelRegistry channels) {
        return DunningReportingServiceGrpc.newBlockingStub(channels.lookup("acme.billing.v1.DunningReportingService"));
    }

    @Bean
    public DunningValidationServiceGrpc.DunningValidationServiceBlockingStub billingDunningValidationServiceBlocking(ChannelRegistry channels) {
        return DunningValidationServiceGrpc.newBlockingStub(channels.lookup("acme.billing.v1.DunningValidationService"));
    }

    @Bean
    public InvoiceApprovalServiceGrpc.InvoiceApprovalServiceBlockingStub billingInvoiceApprovalServiceBlocking(ChannelRegistry channels) {
        return InvoiceApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.billing.v1.InvoiceApprovalService"));
    }

    @Bean
    public InvoiceCalculationServiceGrpc.InvoiceCalculationServiceBlockingStub billingInvoiceCalculationServiceBlocking(ChannelRegistry channels) {
        return InvoiceCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.billing.v1.InvoiceCalculationService"));
    }

    @Bean
    public InvoiceCalculationServiceGrpc.InvoiceCalculationServiceFutureStub billingInvoiceCalculationServiceFuture(ChannelRegistry channels) {
        return InvoiceCalculationServiceGrpc.newFutureStub(channels.lookup("acme.billing.v1.InvoiceCalculationService"));
    }

    @Bean
    public InvoiceQueryServiceGrpc.InvoiceQueryServiceBlockingStub billingInvoiceQueryServiceBlocking(ChannelRegistry channels) {
        return InvoiceQueryServiceGrpc.newBlockingStub(channels.lookup("acme.billing.v1.InvoiceQueryService"));
    }

    @Bean
    public InvoiceValidationServiceGrpc.InvoiceValidationServiceBlockingStub billingInvoiceValidationServiceBlocking(ChannelRegistry channels) {
        return InvoiceValidationServiceGrpc.newBlockingStub(channels.lookup("acme.billing.v1.InvoiceValidationService"));
    }

    @Bean
    public InvoiceValidationServiceGrpc.InvoiceValidationServiceFutureStub billingInvoiceValidationServiceFuture(ChannelRegistry channels) {
        return InvoiceValidationServiceGrpc.newFutureStub(channels.lookup("acme.billing.v1.InvoiceValidationService"));
    }

    @Bean
    public InvoiceWorkflowServiceGrpc.InvoiceWorkflowServiceBlockingStub billingInvoiceWorkflowServiceBlocking(ChannelRegistry channels) {
        return InvoiceWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.billing.v1.InvoiceWorkflowService"));
    }

    @Bean
    public PaymentTermApprovalServiceGrpc.PaymentTermApprovalServiceBlockingStub billingPaymentTermApprovalServiceBlocking(ChannelRegistry channels) {
        return PaymentTermApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.billing.v1.PaymentTermApprovalService"));
    }

    @Bean
    public PaymentTermCalculationServiceGrpc.PaymentTermCalculationServiceStub billingPaymentTermCalculationServiceAsync(ChannelRegistry channels) {
        return PaymentTermCalculationServiceGrpc.newStub(channels.lookup("acme.billing.v1.PaymentTermCalculationService"));
    }

    @Bean
    public PaymentTermCalculationServiceGrpc.PaymentTermCalculationServiceBlockingStub billingPaymentTermCalculationServiceBlocking(ChannelRegistry channels) {
        return PaymentTermCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.billing.v1.PaymentTermCalculationService"));
    }

    @Bean
    public PaymentTermCalculationServiceGrpc.PaymentTermCalculationServiceFutureStub billingPaymentTermCalculationServiceFuture(ChannelRegistry channels) {
        return PaymentTermCalculationServiceGrpc.newFutureStub(channels.lookup("acme.billing.v1.PaymentTermCalculationService"));
    }

    @Bean
    public PaymentTermLifecycleServiceGrpc.PaymentTermLifecycleServiceBlockingStub billingPaymentTermLifecycleServiceBlocking(ChannelRegistry channels) {
        return PaymentTermLifecycleServiceGrpc.newBlockingStub(channels.lookup("acme.billing.v1.PaymentTermLifecycleService"));
    }

    @Bean
    public PaymentTermValidationServiceGrpc.PaymentTermValidationServiceStub billingPaymentTermValidationServiceAsync(ChannelRegistry channels) {
        return PaymentTermValidationServiceGrpc.newStub(channels.lookup("acme.billing.v1.PaymentTermValidationService"));
    }

    @Bean
    public PaymentTermValidationServiceGrpc.PaymentTermValidationServiceBlockingStub billingPaymentTermValidationServiceBlocking(ChannelRegistry channels) {
        return PaymentTermValidationServiceGrpc.newBlockingStub(channels.lookup("acme.billing.v1.PaymentTermValidationService"));
    }
}
