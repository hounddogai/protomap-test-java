package com.acme.finance.domains.expenses;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.NotifyRetentionPolicyWatchersRequest;
import com.acme.audit.v1.NotifyRetentionPolicyWatchersResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicyExportServiceGrpc;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.RetryRetentionPolicyExportRequest;
import com.acme.audit.v1.RetryRetentionPolicyExportResponse;
import com.acme.audit.v1.RevertRetentionPolicyRequest;
import com.acme.audit.v1.RevertRetentionPolicyResponse;
import com.acme.audit.v1.ScoreAuditEventRequest;
import com.acme.audit.v1.ScoreAuditEventResponse;
import com.acme.audit.v1.StreamRetentionPolicyExportRequest;
import com.acme.audit.v1.StreamRetentionPolicyExportResponse;
import com.acme.budgeting.v1.DelegateVarianceApprovalRequest;
import com.acme.budgeting.v1.DelegateVarianceApprovalResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceApprovalServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CalculateHouseholdRequest;
import com.acme.customers.v1.CalculateHouseholdResponse;
import com.acme.customers.v1.ComparePreferenceOptionsRequest;
import com.acme.customers.v1.ComparePreferenceOptionsResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordCalculationServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.NormalizePreferenceRequest;
import com.acme.customers.v1.NormalizePreferenceResponse;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.QuoteConsentRecordRequest;
import com.acme.customers.v1.QuoteConsentRecordResponse;
import com.acme.expenses.v1.ActivateMileageClaimRequest;
import com.acme.expenses.v1.ActivateMileageClaimResponse;
import com.acme.expenses.v1.CompareReceiptOptionsRequest;
import com.acme.expenses.v1.CompareReceiptOptionsResponse;
import com.acme.expenses.v1.CountExpenseReportsRequest;
import com.acme.expenses.v1.CountExpenseReportsResponse;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportLine;
import com.acme.expenses.v1.ExpenseReportLineTax;
import com.acme.expenses.v1.ExpenseReportQueryServiceGrpc;
import com.acme.expenses.v1.ExpenseReportServiceGrpc;
import com.acme.expenses.v1.ExpireMileageClaimRequest;
import com.acme.expenses.v1.ExpireMileageClaimResponse;
import com.acme.expenses.v1.GetMileageClaimLifecycleRequest;
import com.acme.expenses.v1.GetMileageClaimLifecycleResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.MileageClaimLine;
import com.acme.expenses.v1.MileageClaimRef;
import com.acme.expenses.v1.MileageClaimReportingServiceGrpc;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptCalculationServiceGrpc;
import com.acme.expenses.v1.ReceiptRef;
import com.acme.expenses.v1.ReinstateMileageClaimRequest;
import com.acme.expenses.v1.ReinstateMileageClaimResponse;
import com.acme.expenses.v1.RenewMileageClaimRequest;
import com.acme.expenses.v1.RenewMileageClaimResponse;
import com.acme.expenses.v1.ScheduleMileageClaimExpiryRequest;
import com.acme.expenses.v1.ScheduleMileageClaimExpiryResponse;
import com.acme.expenses.v1.ShareMileageClaimReportRequest;
import com.acme.expenses.v1.ShareMileageClaimReportResponse;
import com.acme.expenses.v1.SuspendMileageClaimRequest;
import com.acme.expenses.v1.SuspendMileageClaimResponse;
import com.acme.expenses.v1.TerminateMileageClaimRequest;
import com.acme.expenses.v1.TerminateMileageClaimResponse;
import com.acme.expenses.v1.UpdateExpenseReportRequest;
import com.acme.expenses.v1.UpdateExpenseReportResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.CloneUserRequest;
import com.acme.identity.v1.CloneUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryLine;
import com.acme.ledger.v1.JournalEntryLineTax;
import com.acme.ledger.v1.JournalEntryValidationServiceGrpc;
import com.acme.ledger.v1.LedgerAccountRef;
import com.acme.ledger.v1.PreviewJournalEntryRequest;
import com.acme.ledger.v1.PreviewJournalEntryResponse;
import com.acme.search.v1.ExportSearchIndexesRequest;
import com.acme.search.v1.ExportSearchIndexesResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.acme.sponsorships.v1.SponsorQueryServiceGrpc;
import com.acme.sponsorships.v1.SummarizeSponsorsRequest;
import com.acme.sponsorships.v1.SummarizeSponsorsResponse;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class MileageClaimLifecycleRequestMapperImpl implements MileageClaimLifecycleRequestMapper {
    @Override
    public RetryRetentionPolicyExportRequest expireMileageClaimStage0(ExpireMileageClaimRequest request) {
        return RetryRetentionPolicyExportRequest.newBuilder()
                .setContext(request.getContext())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .build())
                .build();
    }
}
