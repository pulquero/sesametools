package net.fortytwo.sesametools.deduplication;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.StatementImpl;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class DuplicateStatementFinder {

    public static Set<Statement> findDuplicateStatements(final SailConnection sc) throws SailException {
        boolean includeInferred = false;

        // The HashSet is safe because none of the statements we'll add have a
        // non-null named analysis context.
        Set<Statement> results = new HashSet<Statement>();

        CloseableIteration<? extends Resource, SailException> contexts
                = sc.getContextIDs();
        try {
            while (contexts.hasNext()) {
                Resource ctx = contexts.next();
                if (null != ctx) {
                    CloseableIteration<? extends Statement, SailException> stmts
                            = sc.getStatements(null, null, null, includeInferred, ctx);
                    
                    try {
                        while (stmts.hasNext()) {
                            Statement st = stmts.next();

                            CloseableIteration<? extends Statement, SailException> dups
                                    = sc.getStatements(
                                    st.getSubject(), st.getPredicate(), st.getObject(), includeInferred);
                            try {
                                int count = 0;
                                while (dups.hasNext()) {
                                    count++;
                                    if (2 == count) {
                                        Statement dup = new StatementImpl(
                                                st.getSubject(), st.getPredicate(), st.getObject());
                                        results.add(dup);
                                        break;
                                    }

                                    dups.next();
                                }
                            } finally {
                                dups.close();
                            }
                        }
                    } finally {
                        stmts.close();
                    }
                }
            }
        } finally {
            contexts.close();
        }

        return results;
    }
}
