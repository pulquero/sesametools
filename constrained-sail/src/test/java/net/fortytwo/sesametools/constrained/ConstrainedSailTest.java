
package net.fortytwo.sesametools.constrained;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.impl.DatasetImpl;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.memory.MemoryStore;

import junit.framework.TestCase;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class ConstrainedSailTest extends TestCase {
    private static final IRI
            CONTEXT1_RW = SimpleValueFactory.getInstance().createIRI("http://example.org/context1"),
            CONTEXT2_R = SimpleValueFactory.getInstance().createIRI("http://example.org/context2"),
            CONTEXT3_W = SimpleValueFactory.getInstance().createIRI("http://example.org/context3");

    private Sail baseSail;
    private ConstrainedSail constrainedSail;

    public void setUp() throws Exception {
        DatasetImpl writableSet = new DatasetImpl();
        writableSet.addDefaultGraph(CONTEXT1_RW);
        writableSet.addDefaultGraph(CONTEXT3_W);

        DatasetImpl readableSet = new DatasetImpl();
        readableSet.addDefaultGraph(CONTEXT1_RW);
        readableSet.addDefaultGraph(CONTEXT2_R);

        baseSail = new MemoryStore();
        baseSail.initialize();

        boolean hideNonWritableContexts = false;
        constrainedSail = new ConstrainedSail(baseSail, readableSet, writableSet, CONTEXT3_W, hideNonWritableContexts);
        constrainedSail.initialize();
    }

    public void tearDown() throws Exception {
        constrainedSail.shutDown();
        baseSail.shutDown();
    }

    // Note: if WILDCARD_REMOVE_FROM_ALL_CONTEXTS is ever made into a variable
    // (and set to false), we'll need a different test.
    public void testWildcardDelete() throws Exception {
        SailConnection sc;

        // Add a statement to each named analysis, below the ConstrainedSail
        sc = baseSail.getConnection();
        sc.begin();
        sc.addStatement(RDF.TYPE, RDF.TYPE, RDF.TYPE, CONTEXT1_RW, CONTEXT2_R, CONTEXT3_W);
        sc.commit();
        assertEquals(3, count(sc.getStatements(RDF.TYPE, null, null, false)));
        assertEquals(1, count(sc.getStatements(RDF.TYPE, null, null, false, CONTEXT1_RW)));
        assertEquals(1, count(sc.getStatements(RDF.TYPE, null, null, false, CONTEXT2_R)));
        assertEquals(1, count(sc.getStatements(RDF.TYPE, null, null, false, CONTEXT3_W)));
        sc.close();

        CloseableIteration<? extends Statement, SailException> iter;
        sc = constrainedSail.getConnection();
        // We can only see the statements in the readable named graphs
        assertEquals(2, count(sc.getStatements(RDF.TYPE, null, null, false)));
        assertEquals(1, count(sc.getStatements(RDF.TYPE, null, null, false, CONTEXT1_RW)));
        assertEquals(1, count(sc.getStatements(RDF.TYPE, null, null, false, CONTEXT2_R)));
        assertEquals(0, count(sc.getStatements(RDF.TYPE, null, null, false, CONTEXT3_W)));
        // Wildcard remove of the added statements
        sc.begin();
        sc.removeStatements(RDF.TYPE, null, null);
        sc.commit();
        // We still see the statement in the readable but non-writable named analysis
        assertEquals(1, count(sc.getStatements(RDF.TYPE, null, null, false)));
        assertEquals(0, count(sc.getStatements(RDF.TYPE, null, null, false, CONTEXT1_RW)));
        assertEquals(1, count(sc.getStatements(RDF.TYPE, null, null, false, CONTEXT2_R)));
        assertEquals(0, count(sc.getStatements(RDF.TYPE, null, null, false, CONTEXT3_W)));
        sc.close();

        sc = baseSail.getConnection();
        // Statements were in fact removed from both writable named graphs
        assertEquals(1, count(sc.getStatements(RDF.TYPE, null, null, false)));
        assertEquals(0, count(sc.getStatements(RDF.TYPE, null, null, false, CONTEXT1_RW)));
        assertEquals(1, count(sc.getStatements(RDF.TYPE, null, null, false, CONTEXT2_R)));
        assertEquals(0, count(sc.getStatements(RDF.TYPE, null, null, false, CONTEXT3_W)));
        sc.close();

        // No statements are removed even if there are no matching statements in
        // any writable named analysis (regression tests for a bug in which zero
        // writable contexts for matching statements leads to a zero-length
        // array vararg argument, which then matches *all* statements).
        sc = constrainedSail.getConnection();
        sc.begin();
        sc.removeStatements(RDF.TYPE, null, null);
        sc.commit();
        assertEquals(1, count(sc.getStatements(RDF.TYPE, null, null, false)));
        assertEquals(0, count(sc.getStatements(RDF.TYPE, null, null, false, CONTEXT1_RW)));
        assertEquals(1, count(sc.getStatements(RDF.TYPE, null, null, false, CONTEXT2_R)));
        assertEquals(0, count(sc.getStatements(RDF.TYPE, null, null, false, CONTEXT3_W)));
        sc.close();
    }

    private int count(final CloseableIteration<? extends Statement, SailException> iter) throws SailException {
        int c = 0;
//System.out.println("...");
        try {
            while (iter.hasNext()) {
                c++;
                iter.next();
//System.out.println(iter.next().toString());
            }
        } finally {
            iter.close();
        }

        return c;
    }
}
