package net.fortytwo.sesametools.ldserver;

import net.fortytwo.sesametools.mappingsail.MappingSail;
import net.fortytwo.sesametools.mappingsail.MappingSchema;
import net.fortytwo.sesametools.mappingsail.RewriteRule;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.sail.Sail;
import org.restlet.Application;

/**
 * A RESTful web service which publishes the contents of a Sail data store as Linked Data.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class LinkedDataServer extends Application {

    private final Sail sail;
    private final IRI datasetIRI;

    private static LinkedDataServer SINGLETON = null;

    /**
     * @param baseSail        the data store published by this server
     * @param internalBaseIRI the base IRI of resources within the data store
     * @param externalBaseIRI the base IRI of resources as they are to be seen in the Linked Data
     */
    public LinkedDataServer(final Sail baseSail,
                            final String internalBaseIRI,
                            final String externalBaseIRI) {
        this(baseSail, internalBaseIRI, externalBaseIRI, null);
    }

    /**
     * @param baseSail        the data store published by this server
     * @param internalBaseIRI the base IRI of resources within the data store
     * @param externalBaseIRI the base IRI of resources as they are to be seen in the Linked Data
     * @param dataset         the IRI of the data set to be published.
     *                        This allows resource descriptions to be associated
     *                        with metadata about the data set which contains them.
     */
    public LinkedDataServer(final Sail baseSail,
                            final String internalBaseIRI,
                            final String externalBaseIRI,
                            final String dataset) {
        if (null != SINGLETON) {
            throw new IllegalStateException("only one LinkedDataServer may be instantiated per JVM");
        }

        SINGLETON = this;

        final ValueFactory vf = baseSail.getValueFactory();

        if (!internalBaseIRI.equals(externalBaseIRI)) {
            RewriteRule outboundRewriter = new RewriteRule() {
                public IRI rewrite(final IRI original) {
                    //System.out.println("outbound: " + original);

                    if (null == original) {
                        return null;
                    } else {
                        String s = original.stringValue();
                        //System.out.println("\t--> " + (s.startsWith(internalBaseIRI)
                        //        ? vf.createIRI(s.replace(internalBaseIRI, externalBaseIRI))
                        //        : original));
                        return s.startsWith(internalBaseIRI)
                                ? vf.createIRI(s.replace(internalBaseIRI, externalBaseIRI))
                                : original;
                    }
                }
            };

            RewriteRule inboundRewriter = new RewriteRule() {
                public IRI rewrite(final IRI original) {
                    //System.out.println("inbound: " + original);
                    if (null == original) {
                        return null;
                    } else {
                        String s = original.stringValue();
                        //System.out.println("\t--> " + (s.startsWith(externalBaseIRI)
                        //        ? vf.createIRI(s.replace(externalBaseIRI, internalBaseIRI))
                        //        : original));
                        return s.startsWith(externalBaseIRI)
                                ? vf.createIRI(s.replace(externalBaseIRI, internalBaseIRI))
                                : original;
                    }
                }
            };

            MappingSchema schema = new MappingSchema();
            schema.setRewriter(MappingSchema.Direction.INBOUND, inboundRewriter);
            schema.setRewriter(MappingSchema.Direction.OUTBOUND, outboundRewriter);
            this.sail = new MappingSail(baseSail, schema);

            datasetIRI = null == dataset
                    ? null
                    : outboundRewriter.rewrite(vf.createIRI(dataset));
        } else {
            this.sail = baseSail;
            datasetIRI = null == dataset
                    ? null
                    : vf.createIRI(dataset);
        }
    }

    /**
     * @return the data store published by this server
     */
    public Sail getSail() {
        return sail;
    }

    /**
     * @return the internal IRI for the data set published by this server
     */
    public IRI getDatasetIRI() {
        return datasetIRI;
    }

    /**
     * @return the single Linked Data server
     */
    public static LinkedDataServer getInstance() {
        return SINGLETON;
    }
}
