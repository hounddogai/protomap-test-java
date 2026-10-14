package com.acme.finance.platform.clients;

import com.acme.banking.v1.BankAccountQueryServiceGrpc;
import com.acme.banking.v1.BankAccountServiceGrpc;
import com.acme.banking.v1.BankAccountWorkflowServiceGrpc;
import com.acme.banking.v1.MandateApprovalServiceGrpc;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.banking.v1.MandateValidationServiceGrpc;
import com.acme.banking.v1.StandingOrderApprovalServiceGrpc;
import com.acme.banking.v1.StandingOrderValidationServiceGrpc;
import com.acme.banking.v1.StandingOrderWorkflowServiceGrpc;
import com.acme.banking.v1.StatementApprovalServiceGrpc;
import com.acme.banking.v1.StatementQueryServiceGrpc;
import com.acme.banking.v1.StatementWorkflowServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the banking contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class BankingClients {
    @Bean
    public BankAccountQueryServiceGrpc.BankAccountQueryServiceBlockingStub bankingBankAccountQueryServiceBlocking(ChannelRegistry channels) {
        return BankAccountQueryServiceGrpc.newBlockingStub(channels.lookup("acme.banking.v1.BankAccountQueryService"));
    }

    @Bean
    public BankAccountQueryServiceGrpc.BankAccountQueryServiceFutureStub bankingBankAccountQueryServiceFuture(ChannelRegistry channels) {
        return BankAccountQueryServiceGrpc.newFutureStub(channels.lookup("acme.banking.v1.BankAccountQueryService"));
    }

    @Bean
    public BankAccountServiceGrpc.BankAccountServiceStub bankingBankAccountServiceAsync(ChannelRegistry channels) {
        return BankAccountServiceGrpc.newStub(channels.lookup("acme.banking.v1.BankAccountService"));
    }

    @Bean
    public BankAccountServiceGrpc.BankAccountServiceBlockingStub bankingBankAccountServiceBlocking(ChannelRegistry channels) {
        return BankAccountServiceGrpc.newBlockingStub(channels.lookup("acme.banking.v1.BankAccountService"));
    }

    @Bean
    public BankAccountWorkflowServiceGrpc.BankAccountWorkflowServiceBlockingStub bankingBankAccountWorkflowServiceBlocking(ChannelRegistry channels) {
        return BankAccountWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.banking.v1.BankAccountWorkflowService"));
    }

    @Bean
    public BankAccountWorkflowServiceGrpc.BankAccountWorkflowServiceFutureStub bankingBankAccountWorkflowServiceFuture(ChannelRegistry channels) {
        return BankAccountWorkflowServiceGrpc.newFutureStub(channels.lookup("acme.banking.v1.BankAccountWorkflowService"));
    }

    @Bean
    public MandateApprovalServiceGrpc.MandateApprovalServiceStub bankingMandateApprovalServiceAsync(ChannelRegistry channels) {
        return MandateApprovalServiceGrpc.newStub(channels.lookup("acme.banking.v1.MandateApprovalService"));
    }

    @Bean
    public MandateApprovalServiceGrpc.MandateApprovalServiceBlockingStub bankingMandateApprovalServiceBlocking(ChannelRegistry channels) {
        return MandateApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.banking.v1.MandateApprovalService"));
    }

    @Bean
    public MandateApprovalServiceGrpc.MandateApprovalServiceFutureStub bankingMandateApprovalServiceFuture(ChannelRegistry channels) {
        return MandateApprovalServiceGrpc.newFutureStub(channels.lookup("acme.banking.v1.MandateApprovalService"));
    }

    @Bean
    public MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking(ChannelRegistry channels) {
        return MandateCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.banking.v1.MandateCalculationService"));
    }

    @Bean
    public MandateValidationServiceGrpc.MandateValidationServiceStub bankingMandateValidationServiceAsync(ChannelRegistry channels) {
        return MandateValidationServiceGrpc.newStub(channels.lookup("acme.banking.v1.MandateValidationService"));
    }

    @Bean
    public MandateValidationServiceGrpc.MandateValidationServiceBlockingStub bankingMandateValidationServiceBlocking(ChannelRegistry channels) {
        return MandateValidationServiceGrpc.newBlockingStub(channels.lookup("acme.banking.v1.MandateValidationService"));
    }

    @Bean
    public StandingOrderApprovalServiceGrpc.StandingOrderApprovalServiceStub bankingStandingOrderApprovalServiceAsync(ChannelRegistry channels) {
        return StandingOrderApprovalServiceGrpc.newStub(channels.lookup("acme.banking.v1.StandingOrderApprovalService"));
    }

    @Bean
    public StandingOrderApprovalServiceGrpc.StandingOrderApprovalServiceBlockingStub bankingStandingOrderApprovalServiceBlocking(ChannelRegistry channels) {
        return StandingOrderApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.banking.v1.StandingOrderApprovalService"));
    }

    @Bean
    public StandingOrderValidationServiceGrpc.StandingOrderValidationServiceBlockingStub bankingStandingOrderValidationServiceBlocking(ChannelRegistry channels) {
        return StandingOrderValidationServiceGrpc.newBlockingStub(channels.lookup("acme.banking.v1.StandingOrderValidationService"));
    }

    @Bean
    public StandingOrderValidationServiceGrpc.StandingOrderValidationServiceFutureStub bankingStandingOrderValidationServiceFuture(ChannelRegistry channels) {
        return StandingOrderValidationServiceGrpc.newFutureStub(channels.lookup("acme.banking.v1.StandingOrderValidationService"));
    }

    @Bean
    public StandingOrderWorkflowServiceGrpc.StandingOrderWorkflowServiceBlockingStub bankingStandingOrderWorkflowServiceBlocking(ChannelRegistry channels) {
        return StandingOrderWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.banking.v1.StandingOrderWorkflowService"));
    }

    @Bean
    public StandingOrderWorkflowServiceGrpc.StandingOrderWorkflowServiceFutureStub bankingStandingOrderWorkflowServiceFuture(ChannelRegistry channels) {
        return StandingOrderWorkflowServiceGrpc.newFutureStub(channels.lookup("acme.banking.v1.StandingOrderWorkflowService"));
    }

    @Bean
    public StatementApprovalServiceGrpc.StatementApprovalServiceBlockingStub bankingStatementApprovalServiceBlocking(ChannelRegistry channels) {
        return StatementApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.banking.v1.StatementApprovalService"));
    }

    @Bean
    public StatementApprovalServiceGrpc.StatementApprovalServiceFutureStub bankingStatementApprovalServiceFuture(ChannelRegistry channels) {
        return StatementApprovalServiceGrpc.newFutureStub(channels.lookup("acme.banking.v1.StatementApprovalService"));
    }

    @Bean
    public StatementQueryServiceGrpc.StatementQueryServiceBlockingStub bankingStatementQueryServiceBlocking(ChannelRegistry channels) {
        return StatementQueryServiceGrpc.newBlockingStub(channels.lookup("acme.banking.v1.StatementQueryService"));
    }

    @Bean
    public StatementWorkflowServiceGrpc.StatementWorkflowServiceBlockingStub bankingStatementWorkflowServiceBlocking(ChannelRegistry channels) {
        return StatementWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.banking.v1.StatementWorkflowService"));
    }
}
