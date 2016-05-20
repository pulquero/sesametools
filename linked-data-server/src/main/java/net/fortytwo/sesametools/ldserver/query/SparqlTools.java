package net.fortytwo.sesametools.ldserver.query;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultWriter;
import org.eclipse.rdf4j.query.resultio.sparqljson.SPARQLResultsJSONWriter;
import org.eclipse.rdf4j.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.restlet.data.MediaType;
import org.restlet.representation.Variant;

import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SparqlTools {
    public enum SparqlResultFormat {
        // Note: the XML format is defined first, so that it is the default format.
        XML("application/sparql-results+xml"),
        JSON("application/sparql-results+json");

        private static List<Variant> VARIANTS;

        private final MediaType mediaType;

        private SparqlResultFormat(final String mimeType) {
            mediaType = new MediaType(mimeType);
        }

        public MediaType getMediaType() {
            return mediaType;
        }

        public static SparqlResultFormat lookup(final MediaType mediaType) {
            for (SparqlResultFormat f : SparqlResultFormat.values()) {
                if (f.mediaType.equals(mediaType)) {
                    return f;
                }
            }

            return null;
        }

        public static List<Variant> getVariants() {
            if (null == VARIANTS) {
                VARIANTS = new LinkedList<Variant>();
                for (SparqlResultFormat f : SparqlResultFormat.values()) {
                    VARIANTS.add(new Variant(f.mediaType));
                }
            }

            return VARIANTS;
        }
    }

    private static final String BASE_IRI = "http://example.org/bogusBaseIRI";

    private static ParsedQuery parseQuery(final String query) throws MalformedQueryException {
        SPARQLParser parser = new SPARQLParser();
        return parser.parseQuery(query, BASE_IRI);

    }

    public static synchronized CloseableIteration<? extends BindingSet, QueryEvaluationException>
    evaluateQuery(final ParsedQuery query,
                  final SailConnection sc) throws QueryException {
        MapBindingSet bindings = new MapBindingSet();
        boolean includeInferred = false;
        try {
            return sc.evaluate(query.getTupleExpr(), query.getDataset(), bindings, includeInferred);
        } catch (SailException e) {
            throw new QueryException(e);
        }
    }

    public static void executeQuery(final String queryStr,
                                    final SailConnection sc,
                                    final OutputStream out,
                                    final int limit,
                                    final SparqlResultFormat format) throws QueryException {
        TupleQueryResultWriter w;

        switch (format) {
            case JSON:
                w = new SPARQLResultsJSONWriter(out);
                break;
            case XML:
                w = new SPARQLResultsXMLWriter(out);
                break;
            default:
                throw new QueryException(new Throwable("bad query result format: " + format));
        }

        ParsedQuery query;

        try {
            query = parseQuery(queryStr);
        } catch (MalformedQueryException e) {
            throw new QueryException(e);
        }

        List<String> columnHeaders = new LinkedList<String>();
        columnHeaders.addAll(query.getTupleExpr().getBindingNames());
        // FIXME: *do* specify the column headers
        //columnHeaders.add("post");
        //columnHeaders.add("content");
        //columnHeaders.add("screen_name");
        try {
            w.startQueryResult(columnHeaders);
        } catch (TupleQueryResultHandlerException e) {
            throw new QueryException(e);
        }

        CloseableIteration<? extends BindingSet, QueryEvaluationException> iter
                = evaluateQuery(query, sc);
        int count = 0;
        try {
            try {
                while (iter.hasNext() && count < limit) {
                    w.handleSolution(iter.next());
                    count++;
                }
            } finally {
                iter.close();
            }

            w.endQueryResult();
        } catch (QueryEvaluationException e) {
            throw new QueryException(e);
        } catch (TupleQueryResultHandlerException e) {
            throw new QueryException(e);
        }
    }
}
