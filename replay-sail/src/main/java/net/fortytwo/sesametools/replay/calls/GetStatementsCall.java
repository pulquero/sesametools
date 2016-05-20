
package net.fortytwo.sesametools.replay.calls;

import net.fortytwo.sesametools.replay.SailConnectionCall;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;

import java.util.StringTokenizer;

/**
 * @author Joshua Shinavier (http://fortytwo.net).
 */
public class GetStatementsCall extends SailConnectionCall<SailConnection, CloseableIteration> {

    private final Resource subject;
    private final IRI predicate;
    private final Value object;
    private final boolean includeInferred;
    private final Resource[] contexts;

    public GetStatementsCall(final String id,
                             final Resource subj,
                             final IRI pred,
                             final Value obj,
                             final boolean includeInferred,
                             final Resource... contexts) {
        super(id, Type.GET_STATEMENTS);
        this.subject = subj;
        this.predicate = pred;
        this.object = obj;
        this.includeInferred = includeInferred;
        this.contexts = contexts;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder(id).append(DELIM).append(type)
                .append(DELIM).append(toString(subject))
                .append(DELIM).append(toString(predicate))
                .append(DELIM).append(toString(object))
                .append(DELIM).append(toString(includeInferred))
                .append(DELIM).append(toString(contexts));

        return sb.toString();
    }

    public GetStatementsCall(final String id,
                             final Type type,
                             final StringTokenizer tok) {
        super(id, type);
        this.subject = parseResource(tok.nextToken());
        this.predicate = parseIRI(tok.nextToken());
        this.object = parseValue(tok.nextToken());
        this.includeInferred = parseBoolean(tok.nextToken());
        this.contexts = parseContexts(tok.nextToken());
    }

    public CloseableIteration execute(final SailConnection sc) throws SailException {
        return sc.getStatements(subject, predicate, object, includeInferred, contexts);
    }
}
