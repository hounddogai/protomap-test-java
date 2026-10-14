package com.acme.finance.jobs.exposuresnapshot;

import com.acme.common.v1.Actor;
import com.acme.common.v1.RequestContext;
import com.acme.finance.platform.CallIdentity;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.server.FinanceApplication;
import com.acme.risk.v1.ExposureApprovalServiceGrpc;
import com.acme.risk.v1.GetExposureApprovalStatusRequest;
import com.acme.risk.v1.ListPendingExposuresRequest;
import io.grpc.Context;
import java.util.concurrent.TimeUnit;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Runs a bounded reconciliation pass. Scheduling and tenant selection belong to the job runner.
 */
@Configuration(proxyBeanMethods = false)
@Profile("job-exposuresnapshot")
public class ExposureSnapshot {
    public static void main(String[] args) {
        try (var application = new SpringApplicationBuilder(FinanceApplication.class).profiles("job-exposuresnapshot")
                .properties("grpc.server.port=-1").run(args)) {
            application.getBean(ChannelRegistry.class);
        }
    }

    @Bean
    ApplicationRunner runExposureSnapshot(ChannelRegistry channels, org.springframework.core.env.Environment environment) {
        return arguments -> {
            String tenant = environment.getRequiredProperty("acme.job.tenant");
            var context = RequestContext.newBuilder().setTenantId(tenant)
                    .setActor(Actor.newBuilder().setServiceAccount(true)).build();
            // The channels' forwarding interceptor gives the job's calls its own credential and tenant.
            Context job = Context.current().withValue(CallIdentity.TENANT, tenant).withValue(CallIdentity.AUTHORIZATION,
                    "Bearer " + environment.getRequiredProperty("acme.job.token"));
            Context previous = job.attach();
            try {
                var source = ExposureApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.risk.v1.ExposureApprovalService"));
                var target = ExposureApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.risk.v1.ExposureApprovalService"));
                var page = source.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingExposures(
                        ListPendingExposuresRequest.newBuilder().setContext(context).build());
                for (var record : page.getExposuresList()) {
                    var request = GetExposureApprovalStatusRequest.newBuilder()
        .setContext(context)
        .setExposureId(record.getExposureId())
        .build();
                    target.withDeadlineAfter(8, TimeUnit.SECONDS).getExposureApprovalStatus(request);
                }
            } finally {
                job.detach(previous);
            }
        };
    }
}
