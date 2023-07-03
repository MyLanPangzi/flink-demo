package com.hiscat.flink.prometheus.sd;

import java.util.LinkedList;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class ServiceDiscoveryLoader {
    public static ServiceDiscovery load(String identifier, ClassLoader classLoader) {
        List<ServiceDiscoveryFactory> factories = discoverFactories(classLoader);
        checkNotFoundFactory(factories);
        List<ServiceDiscoveryFactory> matches = factories.stream().filter(sd -> sd.identifier().equals(identifier)).collect(toList());
        checkMatchesFactories(identifier, factories, matches);
        return matches.get(0).create();
    }

    private static List<ServiceDiscoveryFactory> discoverFactories(ClassLoader classLoader) {
        try {
            final List<ServiceDiscoveryFactory> result = new LinkedList<>();
            ServiceLoader.load(ServiceDiscoveryFactory.class, classLoader)
                    .iterator()
                    .forEachRemaining(result::add);
            return result;
        } catch (ServiceConfigurationError e) {
            throw new RuntimeException(
                    "Could not load service provider for service discovery factory.", e);
        }
    }

    private static void checkNotFoundFactory(List<ServiceDiscoveryFactory> factories) {
        if (factories.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "Could not find any service discovery factories that implement '%s' in the classpath.",
                            ServiceDiscoveryFactory.class.getName()));
        }
    }

    private static void checkMatchesFactories(String identifier, List<ServiceDiscoveryFactory> factories, List<ServiceDiscoveryFactory> matches) {
        checkNotFoundMatchedFactory(identifier, factories, matches);

        checkFindMultipleFactories(identifier, matches);
    }

    private static void checkNotFoundMatchedFactory(String identifier, List<ServiceDiscoveryFactory> factories, List<ServiceDiscoveryFactory> matches) {
        if (matches.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "Could not find any service discovery factory that can handle identifier '%s' that implements '%s' in the classpath.\n\n"
                                    + "Available factories are:\n\n"
                                    + "%s",
                            identifier,
                            ServiceDiscoveryFactory.class.getName(),
                            factories.stream()
                                    .map(f -> f.getClass().getName())
                                    .distinct()
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }
    }

    private static void checkFindMultipleFactories(String identifier, List<ServiceDiscoveryFactory> matches) {
        if (matches.size() > 1) {
            throw new IllegalStateException(
                    String.format(
                            "Multiple  service discovery  factories can handle identifier '%s' that implement '%s' found in the classpath.\n\n"
                                    + "Ambiguous factory classes are:\n\n"
                                    + "%s",
                            identifier,
                            ServiceDiscoveryFactory.class.getName(),
                            matches.stream()
                                    .map(f -> f.getClass().getName())
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }
    }

}
