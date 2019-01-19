package ru.mail.polis.martyusheva.resolvers;

import one.nio.http.HttpSession;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.martyusheva.cluster.ClusterRequest;

import java.io.IOException;

public interface RequestResolver {
    void resolve(@NotNull final HttpSession session, @NotNull final ClusterRequest query) throws IOException;
}
