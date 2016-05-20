package net.fortytwo.sesametools.mappingsail;

import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.helpers.SailWrapper;

/**
 * A <code>Sail</code> which maps between the internal IRI space of a lower-level data store,
 * and an externally visible IRI space
 * (for example, published Linked Data).
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class MappingSail extends SailWrapper {
    private final MappingSchema schema;

    /**
     * @param baseSail the internal data store
     * @param schema a set of rules for IRI rewriting
     */
    public MappingSail(final Sail baseSail,
                       final MappingSchema schema) {
        this.setBaseSail(baseSail);
        this.schema = schema;
    }

    @Override
    public SailConnection getConnection() throws SailException {
        return new MappingSailConnection(this.getBaseSail().getConnection(), schema, this.getValueFactory());
    }

    @Override
    public boolean isWritable() throws SailException {
        // TODO: handle rewriting for write operations
        return false;
    }
}
