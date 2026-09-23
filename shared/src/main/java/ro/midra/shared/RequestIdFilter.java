package ro.midra.shared;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.UUID;

/**
 * Bounds untrusted request identifiers before forwarding them and including them in logs.
 */
public final class RequestIdFilter extends OncePerRequestFilter {
    public static final String HEADER = "X-Request-ID";

    @Override
    protected void doFilterInternal(
            HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String supplied = request.getHeader(HEADER);
        String id =
                supplied != null && supplied.matches("[a-zA-Z0-9._-]{1,80}")
                        ? supplied
                        : UUID.randomUUID().toString();
        response.setHeader(HEADER, id);
        try (var ignored = MDC.putCloseable("requestId", id)) {
            chain.doFilter(wrap(request, id), response);
        }
    }

    private HttpServletRequest wrap(HttpServletRequest request, String id) {
        return new HttpServletRequestWrapper(request) {
            @Override
            public String getHeader(String name) {
                return HEADER.equalsIgnoreCase(name) ? id : super.getHeader(name);
            }

            @Override
            public Enumeration<String> getHeaders(String name) {
                return HEADER.equalsIgnoreCase(name)
                        ? Collections.enumeration(java.util.List.of(id))
                        : super.getHeaders(name);
            }

            @Override
            public Enumeration<String> getHeaderNames() {
                var names = Collections.list(super.getHeaderNames());
                if (names.stream().noneMatch(HEADER::equalsIgnoreCase)) names.add(HEADER);
                return Collections.enumeration(names);
            }
        };
    }
}
