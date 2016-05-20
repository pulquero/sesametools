package net.fortytwo.sesametools.ldserver;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.restlet.data.MediaType;
import org.restlet.representation.Representation;
import org.restlet.representation.Variant;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

/**
 * Information and non-information resources are distinguished by the suffix of the resource's IRI:
 * information resource IRIs end in .rdf or .trig,
 * while non-information resources have no such suffix
 * (and LinkedDataServer will not make statements about such IRIs).
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

    protected String selfIRI;

    protected String hostIdentifier;
    protected String baseRef;
    protected String subjectResourceIRI;
    protected String typeSpecificId;
    protected WebResourceCategory webResourceCategory;
    protected Sail sail;
    protected RDFFormat format = null;
    protected IRI datasetIRI;

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
        selfIRI = this.getRequest().getResourceRef().toString();

        /*
        System.out.println("selfIRI = " + selfIRI);
        System.out.println("request: " + this.getRequest());
        Request request = this.getRequest();
        System.out.println("baseRef = " + request.getResourceRef().getBaseRef());
        System.out.println("host domain = " + request.getResourceRef().getHostDomain());
        System.out.println("host identifier = " + request.getResourceRef().getHostIdentifier());
        System.out.println("hierarchical part = " + request.getResourceRef().getHierarchicalPart());
        System.out.println("host ref = " + request.getHostRef().toString());
        //*/

        int i = selfIRI.lastIndexOf(".");
        if (i > 0) {
            format = Rio.getParserFormatForFileName(selfIRI);
        }

        if (null == format) {
            webResourceCategory = WebResourceCategory.NonInformationResource;
            getVariants().addAll(RDFMediaTypes.getRDFVariants());
        } else {
            webResourceCategory = WebResourceCategory.InformationResource;
            getVariants().add(RDFMediaTypes.findVariant(format));

            hostIdentifier = this.getRequest().getResourceRef().getHostIdentifier();
            baseRef = this.getRequest().getResourceRef().getBaseRef().toString();
            subjectResourceIRI = selfIRI.substring(0, i);
            typeSpecificId = subjectResourceIRI.substring(baseRef.length());
            datasetIRI = LinkedDataServer.getInstance().getDatasetIRI();
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
            IRI subject = sail.getValueFactory().createIRI(subjectResourceIRI);
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

        getResponse().redirectSeeOther(selfIRI + "." + suffix);

        return null;
    }

    private void addIncidentStatements(final org.eclipse.rdf4j.model.Resource vertex,
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
    private void addSeeAlsoStatements(final org.eclipse.rdf4j.model.Resource subject,
                                      final Collection<Statement> statements,
                                      final SailConnection c,
                                      final ValueFactory vf) throws SailException {
        Set<IRI> contexts = new HashSet<IRI>();
        CloseableIteration<? extends Statement, SailException> iter
                = c.getStatements(subject, null, null, false);
        try {
            while (iter.hasNext()) {
                Statement st = iter.next();
                org.eclipse.rdf4j.model.Resource context = st.getContext();

                if (null != context) {
                    if (context instanceof IRI && context.toString().startsWith(hostIdentifier)) {
                        contexts.add((IRI) context);
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
                org.eclipse.rdf4j.model.Resource context = st.getContext();

                if (null != context) {
                    if (context instanceof IRI && context.toString().startsWith(hostIdentifier)) {
                        contexts.add((IRI) context);
                    }
                }
            }
        } finally {
            iter.close();
        }

        for (IRI r : contexts) {
            statements.add(vf.createStatement(subject, RDFS.SEEALSO, r));
        }
    }

    private void addDocumentMetadata(final Collection<Statement> statements,
                                     final ValueFactory vf) throws SailException {
        // Metadata about the document itself
        IRI docIRI = vf.createIRI(selfIRI);
        statements.add(vf.createStatement(docIRI, RDF.TYPE, vf.createIRI("http://xmlns.com/foaf/0.1/Document")));
        statements.add(vf.createStatement(docIRI, RDFS.LABEL,
                vf.createLiteral("" + format.getName() + " description of resource '"
                        + typeSpecificId + "'")));

        // Note: we go to the trouble of special-casing the dataset IRI, so that
        // it is properly rewritten, along with all other TwitLogic resource
        // IRIs (which are rewritten through the Sail).
        if (null != datasetIRI) {
            statements.add(vf.createStatement(docIRI, RDFS.SEEALSO, datasetIRI));
        }
    }

    private String resourceDescriptor() {
        return "resource";
    }

    private Representation getRDFRepresentation(final IRI subject,
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
