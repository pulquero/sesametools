package net.fortytwo.sesametools.ldserver;

import info.aduna.iteration.CloseableIteration;
import org.openrdf.model.Namespace;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;
import org.restlet.data.MediaType;
import org.restlet.representation.Representation;
import org.restlet.representation.Variant;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Information and non-information resources are distinguished by the suffix of the resource's URI:
 * information resource URIs end in .rdf or .trig,
 * while non-information resources have no such suffix
 * (and LinkedDataServer will not make statements about such URIs).
 * A request for an information resource is fulfilled with the resource itself.  No content negotiation occurs.
 * A request for a non-information resource is fulfilled with a 303-redirect
 * to an information resource of the appropriate media type.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class WebResource extends ServerResource {
    private static final Logger logger = Logger.getLogger(WebResource.class.getName());

    enum WebResourceCategory {
        InformationResource, NonInformationResource
    }

    protected String selfURI;

    protected String hostIdentifier;
    protected String baseRef;
    protected String subjectResourceURI;
    protected String typeSpecificId;
    protected WebResourceCategory webResourceCategory;
    protected Sail sail;
    protected RDFFormat format = null;
    protected URI datasetURI;

    public WebResource() throws Exception {
        super();

        getVariants().addAll(RDFMediaTypes.getRDFVariants());
    }

    public void preprocessingHook() throws Exception {
        // Do nothing unless overridden
    }

    public void postProcessingHook() throws Exception {
        // Do nothing unless overridden
    }

    @Get
    public Representation get(final Variant variant) {
        selfURI = this.getRequest().getResourceRef().toString();

        /*
        System.out.println("selfURI = " + selfURI);
        System.out.println("request: " + this.getRequest());
        Request request = this.getRequest();
        System.out.println("baseRef = " + request.getResourceRef().getBaseRef());
        System.out.println("host domain = " + request.getResourceRef().getHostDomain());
        System.out.println("host identifier = " + request.getResourceRef().getHostIdentifier());
        System.out.println("hierarchical part = " + request.getResourceRef().getHierarchicalPart());
        System.out.println("host ref = " + request.getHostRef().toString());
        //*/

        int i = selfURI.lastIndexOf(".");
        if (i > 0) {
            format = RDFFormat.forFileName(selfURI);
        }

        if (null == format) {
            webResourceCategory = WebResourceCategory.NonInformationResource;
            getVariants().addAll(RDFMediaTypes.getRDFVariants());
        } else {
            webResourceCategory = WebResourceCategory.InformationResource;
            getVariants().add(RDFMediaTypes.findVariant(format));

            hostIdentifier = this.getRequest().getResourceRef().getHostIdentifier();
            baseRef = this.getRequest().getResourceRef().getBaseRef().toString();
            subjectResourceURI = selfURI.substring(0, i);
            typeSpecificId = subjectResourceURI.substring(baseRef.length());
            datasetURI = LinkedDataServer.getInstance().getDatasetURI();
            sail = LinkedDataServer.getInstance().getSail();
        }

        MediaType type = variant.getMediaType();

        switch (webResourceCategory) {
            case InformationResource:
                return representInformationResource();
            case NonInformationResource:
                return representNonInformationResource(type);
            default:
                throw new IllegalStateException("no such resource type: " + webResourceCategory);
        }
    }

    private Representation representInformationResource() {
        try {
            preprocessingHook();
            URI subject = sail.getValueFactory().createURI(subjectResourceURI);
            Representation result = getRDFRepresentation(subject, format);
            postProcessingHook();
            return result;
        } catch (Throwable t) {
            t.printStackTrace();
            return null;
        }
    }

    private Representation representNonInformationResource(final MediaType type) {
        RDFFormat format = RDFMediaTypes.findRdfFormat(type);
        if (null == format) {
            throw new IllegalStateException("no RDF format for media type " + type);
        }
        String suffix = format.getDefaultFileExtension();
        if (null == suffix) {
            throw new IllegalStateException("no suffix for RDF format " + type);
        }

        getResponse().redirectSeeOther(selfURI + "." + suffix);

        return null;
    }

    private void addIncidentStatements(final org.openrdf.model.Resource vertex,
                                       final Collection<Statement> statements,
                                       final SailConnection c) throws SailException {
        // Select outbound statements
        CloseableIteration<? extends Statement, SailException> stIter
                = c.getStatements(vertex, null, null, false);
        try {
            while (stIter.hasNext()) {
                Statement s = stIter.next();
                if (null == s.getContext()) {
                    statements.add(s);
                }
            }
        } finally {
            stIter.close();
        }

        // Select inbound statements
        stIter = c.getStatements(null, null, vertex, false);
        try {
            while (stIter.hasNext()) {
                Statement s = stIter.next();
                if (null == s.getContext()) {
                    statements.add(s);
                }
            }
        } finally {
            stIter.close();
        }
    }

    // Note: a SPARQL query might be more efficient in some applications
    private void addSeeAlsoStatements(final org.openrdf.model.Resource subject,
                                      final Collection<Statement> statements,
                                      final SailConnection c,
                                      final ValueFactory vf) throws SailException {
        Set<URI> contexts = new HashSet<URI>();
        CloseableIteration<? extends Statement, SailException> iter
                = c.getStatements(subject, null, null, false);
        try {
            while (iter.hasNext()) {
                Statement st = iter.next();
                org.openrdf.model.Resource context = st.getContext();

                if (null != context) {
                    if (context instanceof URI && context.toString().startsWith(hostIdentifier)) {
                        contexts.add((URI) context);
                    }
                }
            }
        } finally {
            iter.close();
        }

        iter = c.getStatements(null, null, subject, false);
        try {
            while (iter.hasNext()) {
                Statement st = iter.next();
                org.openrdf.model.Resource context = st.getContext();

                if (null != context) {
                    if (context instanceof URI && context.toString().startsWith(hostIdentifier)) {
                        contexts.add((URI) context);
                    }
                }
            }
        } finally {
            iter.close();
        }

        for (URI r : contexts) {
            statements.add(vf.createStatement(subject, RDFS.SEEALSO, r));
        }
    }

    private void addDocumentMetadata(final Collection<Statement> statements,
                                     final ValueFactory vf) throws SailException {
        // Metadata about the document itself
        URI docURI = vf.createURI(selfURI);
        statements.add(vf.createStatement(docURI, RDF.TYPE, vf.createURI("http://xmlns.com/foaf/0.1/Document")));
        statements.add(vf.createStatement(docURI, RDFS.LABEL,
                vf.createLiteral("" + format.getName() + " description of resource '"
                        + typeSpecificId + "'")));

        // Note: we go to the trouble of special-casing the dataset URI, so that
        // it is properly rewritten, along with all other TwitLogic resource
        // URIs (which are rewritten through the Sail).
        if (null != datasetURI) {
            statements.add(vf.createStatement(docURI, RDFS.SEEALSO, datasetURI));
        }
    }

    private String resourceDescriptor() {
        return "resource";
    }

    private Representation getRDFRepresentation(final URI subject,
                                                final RDFFormat format) {
        try {
            Collection<Namespace> namespaces = new LinkedList<Namespace>();
            Collection<Statement> statements = new LinkedList<Statement>();

            SailConnection c = sail.getConnection();
            try {
                // Add statements incident on the resource itself.
                addIncidentStatements(subject, statements, c);

                // Add virtual statements about named graphs.
                addSeeAlsoStatements(subject, statements, c, sail.getValueFactory());

                // Add virtual statements about the document.
                addDocumentMetadata(statements, sail.getValueFactory());

                // Select namespaces, for human-friendliness
                CloseableIteration<? extends Namespace, SailException> nsIter
                        = c.getNamespaces();
                try {
                    while (nsIter.hasNext()) {
                        namespaces.add(nsIter.next());
                    }
                } finally {
                    nsIter.close();
                }
            } finally {
                c.close();
            }

            return new RDFRepresentation(statements, namespaces, format);

        } catch (Throwable t) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            t.printStackTrace(new PrintStream(bos));

            logger.log(Level.WARNING,
                    "failed to create RDF representation (stack trace follows)\n" + bos.toString(), t);
            return null;
        }
    }
}
