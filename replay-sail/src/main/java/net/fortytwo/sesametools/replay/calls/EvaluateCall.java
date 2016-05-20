
package net.fortytwo.sesametools.replay.calls;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import net.fortytwo.sesametools.EmptyCloseableIteration;
import net.fortytwo.sesametools.replay.SailConnectionCall;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;

import java.util.StringTokenizer;

/**
 * @author Joshua Shinavier (http://fortytwo.net).
 */
public class EvaluateCall extends SailConnectionCall<SailConnection, CloseableIteration> {

    private final boolean includeInferred;

    public EvaluateCall(final String id,
                        final boolean includeInferred) {
        super(id, Type.EVALUATE);
        this.includeInferred = includeInferred;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder(id).append(DELIM).append(type)
                .append(DELIM).append(toString(includeInferred));

        return sb.toString();
    }

    public EvaluateCall(final String id,
                        final Type type,
                        final StringTokenizer tok) {
        super(id, type);
        this.includeInferred = parseBoolean(tok.nextToken());
    }

    public CloseableIteration execute(final SailConnection sc) throws SailException {
        // not enough information to reconstruct an evaluate call
        return new EmptyCloseableIteration();
    }
}
