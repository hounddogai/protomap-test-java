package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.ledger.v1.FiscalPeriodAdminServiceGrpc;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.FiscalPeriodHistoryServiceGrpc;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.JournalEntryHistoryServiceGrpc;
import com.acme.ledger.v1.JournalEntryNotificationServiceGrpc;
import com.acme.ledger.v1.JournalEntryQueryServiceGrpc;
import com.acme.ledger.v1.JournalEntryValidationServiceGrpc;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.LedgerAccountQueryServiceGrpc;
import com.acme.ledger.v1.LedgerAccountServiceGrpc;
import com.acme.ledger.v1.LedgerAccountValidationServiceGrpc;
import com.acme.ledger.v1.PostingBatchAdminServiceGrpc;
import com.acme.ledger.v1.PostingBatchHistoryServiceGrpc;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.PostingBatchValidationServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the ledger contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class LedgerClients {
    @Bean
    public FiscalPeriodAdminServiceGrpc.FiscalPeriodAdminServiceBlockingStub ledgerFiscalPeriodAdminServiceBlocking(ChannelRegistry channels) {
        return FiscalPeriodAdminServiceGrpc.newBlockingStub(channels.lookup("acme.ledger.v1.FiscalPeriodAdminService"));
    }

    @Bean
    public FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking(ChannelRegistry channels) {
        return FiscalPeriodExportServiceGrpc.newBlockingStub(channels.lookup("acme.ledger.v1.FiscalPeriodExportService"));
    }

    @Bean
    public FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceFutureStub ledgerFiscalPeriodExportServiceFuture(ChannelRegistry channels) {
        return FiscalPeriodExportServiceGrpc.newFutureStub(channels.lookup("acme.ledger.v1.FiscalPeriodExportService"));
    }

    @Bean
    public FiscalPeriodHistoryServiceGrpc.FiscalPeriodHistoryServiceBlockingStub ledgerFiscalPeriodHistoryServiceBlocking(ChannelRegistry channels) {
        return FiscalPeriodHistoryServiceGrpc.newBlockingStub(channels.lookup("acme.ledger.v1.FiscalPeriodHistoryService"));
    }

    @Bean
    public FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceStub ledgerFiscalPeriodNotificationServiceAsync(ChannelRegistry channels) {
        return FiscalPeriodNotificationServiceGrpc.newStub(channels.lookup("acme.ledger.v1.FiscalPeriodNotificationService"));
    }

    @Bean
    public FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceBlockingStub ledgerFiscalPeriodNotificationServiceBlocking(ChannelRegistry channels) {
        return FiscalPeriodNotificationServiceGrpc.newBlockingStub(channels.lookup("acme.ledger.v1.FiscalPeriodNotificationService"));
    }

    @Bean
    public FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceFutureStub ledgerFiscalPeriodNotificationServiceFuture(ChannelRegistry channels) {
        return FiscalPeriodNotificationServiceGrpc.newFutureStub(channels.lookup("acme.ledger.v1.FiscalPeriodNotificationService"));
    }

    @Bean
    public JournalEntryHistoryServiceGrpc.JournalEntryHistoryServiceStub ledgerJournalEntryHistoryServiceAsync(ChannelRegistry channels) {
        return JournalEntryHistoryServiceGrpc.newStub(channels.lookup("acme.ledger.v1.JournalEntryHistoryService"));
    }

    @Bean
    public JournalEntryHistoryServiceGrpc.JournalEntryHistoryServiceBlockingStub ledgerJournalEntryHistoryServiceBlocking(ChannelRegistry channels) {
        return JournalEntryHistoryServiceGrpc.newBlockingStub(channels.lookup("acme.ledger.v1.JournalEntryHistoryService"));
    }

    @Bean
    public JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceStub ledgerJournalEntryNotificationServiceAsync(ChannelRegistry channels) {
        return JournalEntryNotificationServiceGrpc.newStub(channels.lookup("acme.ledger.v1.JournalEntryNotificationService"));
    }

    @Bean
    public JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceBlockingStub ledgerJournalEntryNotificationServiceBlocking(ChannelRegistry channels) {
        return JournalEntryNotificationServiceGrpc.newBlockingStub(channels.lookup("acme.ledger.v1.JournalEntryNotificationService"));
    }

    @Bean
    public JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceFutureStub ledgerJournalEntryNotificationServiceFuture(ChannelRegistry channels) {
        return JournalEntryNotificationServiceGrpc.newFutureStub(channels.lookup("acme.ledger.v1.JournalEntryNotificationService"));
    }

    @Bean
    public JournalEntryQueryServiceGrpc.JournalEntryQueryServiceStub ledgerJournalEntryQueryServiceAsync(ChannelRegistry channels) {
        return JournalEntryQueryServiceGrpc.newStub(channels.lookup("acme.ledger.v1.JournalEntryQueryService"));
    }

    @Bean
    public JournalEntryQueryServiceGrpc.JournalEntryQueryServiceBlockingStub ledgerJournalEntryQueryServiceBlocking(ChannelRegistry channels) {
        return JournalEntryQueryServiceGrpc.newBlockingStub(channels.lookup("acme.ledger.v1.JournalEntryQueryService"));
    }

    @Bean
    public JournalEntryQueryServiceGrpc.JournalEntryQueryServiceFutureStub ledgerJournalEntryQueryServiceFuture(ChannelRegistry channels) {
        return JournalEntryQueryServiceGrpc.newFutureStub(channels.lookup("acme.ledger.v1.JournalEntryQueryService"));
    }

    @Bean
    public JournalEntryValidationServiceGrpc.JournalEntryValidationServiceStub ledgerJournalEntryValidationServiceAsync(ChannelRegistry channels) {
        return JournalEntryValidationServiceGrpc.newStub(channels.lookup("acme.ledger.v1.JournalEntryValidationService"));
    }

    @Bean
    public JournalEntryValidationServiceGrpc.JournalEntryValidationServiceBlockingStub ledgerJournalEntryValidationServiceBlocking(ChannelRegistry channels) {
        return JournalEntryValidationServiceGrpc.newBlockingStub(channels.lookup("acme.ledger.v1.JournalEntryValidationService"));
    }

    @Bean
    public JournalEntryValidationServiceGrpc.JournalEntryValidationServiceFutureStub ledgerJournalEntryValidationServiceFuture(ChannelRegistry channels) {
        return JournalEntryValidationServiceGrpc.newFutureStub(channels.lookup("acme.ledger.v1.JournalEntryValidationService"));
    }

    @Bean
    public LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking(ChannelRegistry channels) {
        return LedgerAccountHistoryServiceGrpc.newBlockingStub(channels.lookup("acme.ledger.v1.LedgerAccountHistoryService"));
    }

    @Bean
    public LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceFutureStub ledgerLedgerAccountHistoryServiceFuture(ChannelRegistry channels) {
        return LedgerAccountHistoryServiceGrpc.newFutureStub(channels.lookup("acme.ledger.v1.LedgerAccountHistoryService"));
    }

    @Bean
    public LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceStub ledgerLedgerAccountNotificationServiceAsync(ChannelRegistry channels) {
        return LedgerAccountNotificationServiceGrpc.newStub(channels.lookup("acme.ledger.v1.LedgerAccountNotificationService"));
    }

    @Bean
    public LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking(ChannelRegistry channels) {
        return LedgerAccountNotificationServiceGrpc.newBlockingStub(channels.lookup("acme.ledger.v1.LedgerAccountNotificationService"));
    }

    @Bean
    public LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceFutureStub ledgerLedgerAccountNotificationServiceFuture(ChannelRegistry channels) {
        return LedgerAccountNotificationServiceGrpc.newFutureStub(channels.lookup("acme.ledger.v1.LedgerAccountNotificationService"));
    }

    @Bean
    public LedgerAccountQueryServiceGrpc.LedgerAccountQueryServiceStub ledgerLedgerAccountQueryServiceAsync(ChannelRegistry channels) {
        return LedgerAccountQueryServiceGrpc.newStub(channels.lookup("acme.ledger.v1.LedgerAccountQueryService"));
    }

    @Bean
    public LedgerAccountQueryServiceGrpc.LedgerAccountQueryServiceBlockingStub ledgerLedgerAccountQueryServiceBlocking(ChannelRegistry channels) {
        return LedgerAccountQueryServiceGrpc.newBlockingStub(channels.lookup("acme.ledger.v1.LedgerAccountQueryService"));
    }

    @Bean
    public LedgerAccountServiceGrpc.LedgerAccountServiceBlockingStub ledgerLedgerAccountServiceBlocking(ChannelRegistry channels) {
        return LedgerAccountServiceGrpc.newBlockingStub(channels.lookup("acme.ledger.v1.LedgerAccountService"));
    }

    @Bean
    public LedgerAccountServiceGrpc.LedgerAccountServiceFutureStub ledgerLedgerAccountServiceFuture(ChannelRegistry channels) {
        return LedgerAccountServiceGrpc.newFutureStub(channels.lookup("acme.ledger.v1.LedgerAccountService"));
    }

    @Bean
    public LedgerAccountValidationServiceGrpc.LedgerAccountValidationServiceBlockingStub ledgerLedgerAccountValidationServiceBlocking(ChannelRegistry channels) {
        return LedgerAccountValidationServiceGrpc.newBlockingStub(channels.lookup("acme.ledger.v1.LedgerAccountValidationService"));
    }

    @Bean
    public PostingBatchAdminServiceGrpc.PostingBatchAdminServiceBlockingStub ledgerPostingBatchAdminServiceBlocking(ChannelRegistry channels) {
        return PostingBatchAdminServiceGrpc.newBlockingStub(channels.lookup("acme.ledger.v1.PostingBatchAdminService"));
    }

    @Bean
    public PostingBatchHistoryServiceGrpc.PostingBatchHistoryServiceBlockingStub ledgerPostingBatchHistoryServiceBlocking(ChannelRegistry channels) {
        return PostingBatchHistoryServiceGrpc.newBlockingStub(channels.lookup("acme.ledger.v1.PostingBatchHistoryService"));
    }

    @Bean
    public PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking(ChannelRegistry channels) {
        return PostingBatchNotificationServiceGrpc.newBlockingStub(channels.lookup("acme.ledger.v1.PostingBatchNotificationService"));
    }

    @Bean
    public PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceFutureStub ledgerPostingBatchNotificationServiceFuture(ChannelRegistry channels) {
        return PostingBatchNotificationServiceGrpc.newFutureStub(channels.lookup("acme.ledger.v1.PostingBatchNotificationService"));
    }

    @Bean
    public PostingBatchValidationServiceGrpc.PostingBatchValidationServiceStub ledgerPostingBatchValidationServiceAsync(ChannelRegistry channels) {
        return PostingBatchValidationServiceGrpc.newStub(channels.lookup("acme.ledger.v1.PostingBatchValidationService"));
    }

    @Bean
    public PostingBatchValidationServiceGrpc.PostingBatchValidationServiceBlockingStub ledgerPostingBatchValidationServiceBlocking(ChannelRegistry channels) {
        return PostingBatchValidationServiceGrpc.newBlockingStub(channels.lookup("acme.ledger.v1.PostingBatchValidationService"));
    }
}
