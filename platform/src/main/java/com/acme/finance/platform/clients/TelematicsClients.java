package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.telematics.v1.TelemetryReadingCalculationServiceGrpc;
import com.acme.telematics.v1.TelemetryReadingQueryServiceGrpc;
import com.acme.telematics.v1.TelemetryReadingServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the telematics contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class TelematicsClients {
    @Bean
    public TelemetryReadingCalculationServiceGrpc.TelemetryReadingCalculationServiceBlockingStub telematicsTelemetryReadingCalculationServiceBlocking(ChannelRegistry channels) {
        return TelemetryReadingCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.telematics.v1.TelemetryReadingCalculationService"));
    }

    @Bean
    public TelemetryReadingQueryServiceGrpc.TelemetryReadingQueryServiceBlockingStub telematicsTelemetryReadingQueryServiceBlocking(ChannelRegistry channels) {
        return TelemetryReadingQueryServiceGrpc.newBlockingStub(channels.lookup("acme.telematics.v1.TelemetryReadingQueryService"));
    }

    @Bean
    public TelemetryReadingServiceGrpc.TelemetryReadingServiceBlockingStub telematicsTelemetryReadingServiceBlocking(ChannelRegistry channels) {
        return TelemetryReadingServiceGrpc.newBlockingStub(channels.lookup("acme.telematics.v1.TelemetryReadingService"));
    }
}
