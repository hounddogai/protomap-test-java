package com.acme.finance.domains.expenses;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerServiceGrpc;
import com.acme.customers.v1.ExplainCustomerCalculationRequest;
import com.acme.customers.v1.ExplainCustomerCalculationResponse;
import com.acme.customers.v1.RecalculateCustomerSegmentsRequest;
import com.acme.customers.v1.RecalculateCustomerSegmentsResponse;
import com.acme.customers.v1.UpdateCustomerRequest;
import com.acme.customers.v1.UpdateCustomerResponse;
import com.acme.expenses.v1.CountExpensePolicyTotalsRequest;
import com.acme.expenses.v1.CountExpensePolicyTotalsResponse;
import com.acme.expenses.v1.DeleteExpensePolicyReportRequest;
import com.acme.expenses.v1.DeleteExpensePolicyReportResponse;
import com.acme.expenses.v1.EstimateReceiptRequest;
import com.acme.expenses.v1.EstimateReceiptResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicySyncServiceGrpc;
import com.acme.expenses.v1.ExpensePolicyValidationServiceGrpc;
import com.acme.expenses.v1.FetchExpensePolicyReportRequest;
import com.acme.expenses.v1.FetchExpensePolicyReportResponse;
import com.acme.expenses.v1.GenerateExpensePolicyReportRequest;
import com.acme.expenses.v1.GenerateExpensePolicyReportResponse;
import com.acme.expenses.v1.ListExpensePolicyConflictsRequest;
import com.acme.expenses.v1.ListExpensePolicyConflictsResponse;
import com.acme.expenses.v1.ListExpensePolicyReportsRequest;
import com.acme.expenses.v1.ListExpensePolicyReportsResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.MileageClaimValidationServiceGrpc;
import com.acme.expenses.v1.PreviewExpensePolicyRequest;
import com.acme.expenses.v1.PreviewExpensePolicyResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptLine;
import com.acme.expenses.v1.ScheduleExpensePolicyReportRequest;
import com.acme.expenses.v1.ScheduleExpensePolicyReportResponse;
import com.acme.expenses.v1.StreamExpensePolicyMetricsRequest;
import com.acme.expenses.v1.StreamExpensePolicyMetricsResponse;
import com.acme.expenses.v1.ValidateMileageClaimRequest;
import com.acme.expenses.v1.ValidateMileageClaimResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.fleet.v1.VehicleServiceGrpc;
import com.acme.fleet.v1.WatchVehiclesRequest;
import com.acme.fleet.v1.WatchVehiclesResponse;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageLifecycleServiceGrpc;
import com.acme.insurance.v1.ScheduleCoverageExpiryRequest;
import com.acme.insurance.v1.ScheduleCoverageExpiryResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryBankSettlement;
import com.acme.ledger.v1.JournalEntryValidationServiceGrpc;
import com.acme.ledger.v1.LintJournalEntriesRequest;
import com.acme.ledger.v1.LintJournalEntriesResponse;
import com.acme.notifications.v1.CountNotificationsRequest;
import com.acme.notifications.v1.CountNotificationsResponse;
import com.acme.notifications.v1.NotificationQueryServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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
public final class ExpensePolicyReportingRequestMapperImpl implements ExpensePolicyReportingRequestMapper {
    @Override
    public ExplainCustomerCalculationRequest streamExpensePolicyMetricsStage0(StreamExpensePolicyMetricsRequest request) {
        return ExplainCustomerCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setRelationshipManager(UserRef.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .build())
                        .build())
                .build();
    }
}
