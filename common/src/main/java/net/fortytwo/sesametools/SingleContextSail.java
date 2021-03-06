package net.fortytwo.sesametools;

import org.openrdf.model.Resource;
import org.openrdf.model.ValueFactory;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.StackableSail;
import org.openrdf.sail.helpers.SailBase;

import java.io.File;

/**
 * A Sail which treats the wildcard context as a single, specific context,
 * and disallows read and write access to all other contexts, including the default context.
 * Namespaces may be set and retrieved without restriction.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SingleContextSail extends SailBase implements StackableSail {
    private Sail baseSail;
    private Resource context;

    /**
     * Construct a Sail which is restricted to a single graph context.
     * @param baseSail a base Sail for storage
     * @param context a single, non-null graph context
     */
    public SingleContextSail(final Sail baseSail, final Resource context) {
        this.baseSail = baseSail;
        this.context = context;

        if (null == context) {
            throw new IllegalArgumentException("context may not be null");
        }
    }

    public Sail getBaseSail() {
        return baseSail;
    }

    public void setBaseSail(final Sail baseSail) {
        this.baseSail = baseSail;
    }

    protected SailConnection getConnectionInternal() throws SailException {
        return new SingleContextSailConnection(this, baseSail, context);
    }

    @Override
    public File getDataDir() {
        return baseSail.getDataDir();
    }

    public ValueFactory getValueFactory() {
        return baseSail.getValueFactory();
    }

    protected void initializeInternal() throws SailException {
        baseSail.initialize();
    }

    @Override
    public boolean isWritable() throws SailException {
        return baseSail.isWritable();
    }

    @Override
    public void setDataDir(final File dir) {
        baseSail.setDataDir(dir);
    }

    protected void shutDownInternal() throws SailException {
        baseSail.shutDown();
    }
}
